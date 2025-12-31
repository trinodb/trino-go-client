package trino

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	_ "unsafe" // for go:linkname
)

// PollingConn provides a polling-based interface to Trino queries.
// Unlike the standard sql.DB interface which blocks until the query completes,
// this allows starting a query and polling for results incrementally.
type PollingConn struct {
	conn *Conn
}

// PollingResult contains the results of a query start or poll operation.
type PollingResult struct {
	QueryID string
	// Indicated whether the query has completed
	Finished bool
	// The URI to poll for more results (empty when Finished is true)
	NextURI string

	// Partial results of DQL statements (SELECT, EXPLAIN, EXECUTE) returned in
	// this poll, if any.
	Rows *PollingRows

	// Partial results of DML statements (INSERT, UPDATE, DELETE) returned in
	// this poll, if any.
	UpdateType  string
	UpdateCount int64
}

// PollingColumnType provides type information for a column. It is a copy of database/sql.ColumnType,
// used to parse column types similarly as done in the sync client. We cannot instantiate database/sql.ColumnType
// directly as it has private fields.
type PollingColumnType struct {
	name string

	hasNullable       bool
	hasLength         bool
	hasPrecisionScale bool

	nullable     bool
	length       int64
	databaseType string
	precision    int64
	scale        int64
	scanType     reflect.Type
}

func (t *PollingColumnType) Name() string {
	return t.name
}

func (t *PollingColumnType) DatabaseTypeName() string {
	return t.databaseType
}

func (t *PollingColumnType) DecimalSize() (precision, scale int64, ok bool) {
	return t.precision, t.scale, t.hasPrecisionScale
}

// NewPollingConn creates a new polling connection from a DSN and HTTP client.
func NewPollingConn(dsn string, httpClient *http.Client) (*PollingConn, error) {
	// Register the custom HTTP client if provided
	if httpClient != nil {
		clientKey := "polling_client"
		err := RegisterCustomClient(clientKey, httpClient)
		if err != nil {
			return nil, fmt.Errorf("could not register HTTP client: %w", err)
		}
		// Append custom_client parameter to DSN
		if strings.Contains(dsn, "?") {
			dsn = dsn + "&custom_client=" + clientKey
		} else {
			dsn = dsn + "?custom_client=" + clientKey
		}
	}

	conn, err := newConn(dsn)
	if err != nil {
		return nil, err
	}
	return &PollingConn{conn: conn}, nil
}

// StartQuery starts executing a query and returns any initial results along
// with a NextURI for polling.
func (pc *PollingConn) StartQuery(ctx context.Context, query string, args ...any) (*PollingResult, error) {
	st := &driverStmt{conn: pc.conn, query: query}

	// Convert args to driver.NamedValue format
	driverArgs := convertQueryArgsToDriverArgs(args...)

	// Execute the query start logic (POST /v1/statement)
	qresp, err := pc.startQuery(ctx, st, query, driverArgs)
	if err != nil {
		return nil, err
	}

	return pc.buildPollingResult(qresp)
}

// PollQuery polls for more query results using the NextURI from a previous
// result.
func (pc *PollingConn) PollQuery(ctx context.Context, nextURI string) (*PollingResult, error) {
	st := &driverStmt{conn: pc.conn, query: ""}

	qresp, err := pc.pollQuery(ctx, st, nextURI)
	if err != nil {
		return nil, err
	}

	return pc.buildPollingResult(qresp)
}

// convertQueryArgsToDriverArgs converts query args (including `X-Trino-*` named args)
// to the format expected by the trino driver.
//
// This is useful for users who want to use the polling API with the same arguments
// they would use with sql.DB, such as sql.Named for headers like X-Trino-User.
//
// Loosely based on the logic in database/sql.driverArgsConnLocked.
func convertQueryArgsToDriverArgs(queryArgs ...any) []driver.NamedValue {
	named := make([]driver.NamedValue, len(queryArgs))

	for i, arg := range queryArgs {
		ord := i + 1
		switch v := arg.(type) {
		case driver.NamedValue:
			nv := v
			nv.Ordinal = ord
			named[i] = nv
		case *driver.NamedValue:
			if v == nil {
				named[i] = driver.NamedValue{Ordinal: ord, Value: nil}
			} else {
				nv := *v
				nv.Ordinal = ord
				named[i] = nv
			}
		case sql.NamedArg:
			named[i] = driver.NamedValue{Ordinal: ord, Name: v.Name, Value: v.Value}
		case *sql.NamedArg:
			if v == nil {
				named[i] = driver.NamedValue{Ordinal: ord, Value: nil}
			} else {
				named[i] = driver.NamedValue{Ordinal: ord, Name: v.Name, Value: v.Value}
			}
		default:
			// Plain positional parameter
			named[i] = driver.NamedValue{Ordinal: ord, Value: arg}
		}
	}
	return named
}

// startQuery implements the logic from driverStmt.exec to start a query.
func (pc *PollingConn) startQuery(ctx context.Context, st *driverStmt, query string, args []driver.NamedValue) (*queryResponse, error) {
	hs := make(http.Header)
	hs.Add("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")

	if len(args) > 0 {
		var ss []string
		for _, arg := range args {
			if arg.Name == trinoProgressCallbackParam {
				st.conn.progressUpdater = arg.Value.(ProgressUpdater)
				continue
			}
			if arg.Name == trinoProgressCallbackPeriodParam {
				st.conn.progressUpdaterPeriod.Period = arg.Value.(time.Duration)
				continue
			}

			s, err := Serial(arg.Value)
			if err != nil {
				return nil, err
			}

			if strings.HasPrefix(arg.Name, trinoHeaderPrefix) {
				headerValue := arg.Value.(string)
				if arg.Name == trinoUserHeader {
					st.user = headerValue
				}
				hs.Add(arg.Name, headerValue)
			} else {
				if hs.Get(preparedStatementHeader) == "" {
					for _, v := range st.conn.httpHeaders.Values(preparedStatementHeader) {
						hs.Add(preparedStatementHeader, v)
					}
					hs.Add(preparedStatementHeader, preparedStatementName+"="+url.QueryEscape(st.query))
				}
				ss = append(ss, s)
			}
		}
		if (st.conn.progressUpdater != nil && st.conn.progressUpdaterPeriod.Period == 0) || (st.conn.progressUpdater == nil && st.conn.progressUpdaterPeriod.Period > 0) {
			return nil, ErrInvalidProgressCallbackHeader
		}
		if len(ss) > 0 {
			query = "EXECUTE " + preparedStatementName + " USING " + strings.Join(ss, ", ")
		}
	}

	var cancel context.CancelFunc = func() {}
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, DefaultQueryTimeout)
	}
	defer cancel()

	req, err := st.conn.newRequest(ctx, "POST", st.conn.baseURL+"/v1/statement", strings.NewReader(query), hs)
	if err != nil {
		return nil, err
	}

	resp, err := st.conn.roundTrip(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var qresp queryResponse
	d := json.NewDecoder(resp.Body)
	d.UseNumber()
	err = d.Decode(&qresp)
	if err != nil {
		return nil, fmt.Errorf("trino: %w", err)
	}

	return &qresp, handleResponseError(resp.StatusCode, qresp.Error)
}

// pollQuery implements the polling logic to get more results.
func (pc *PollingConn) pollQuery(ctx context.Context, st *driverStmt, nextURI string) (*queryResponse, error) {
	hs := make(http.Header)
	hs.Add(trinoUserHeader, "trino-polling-client")

	req, err := st.conn.newRequest(ctx, "GET", nextURI, nil, hs)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, context.Canceled
		}
		return nil, err
	}

	resp, err := st.conn.roundTrip(ctx, req)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, context.Canceled
		}
		return nil, err
	}
	defer resp.Body.Close()

	var qresp queryResponse
	d := json.NewDecoder(resp.Body)
	d.UseNumber()
	err = d.Decode(&qresp)
	if err != nil {
		return nil, fmt.Errorf("trino: %w", err)
	}

	return &qresp, handleResponseError(resp.StatusCode, qresp.Error)
}

// buildPollingResult converts internal queryResponse to public PollingResult.
func (pc *PollingConn) buildPollingResult(qresp *queryResponse) (*PollingResult, error) {
	result := &PollingResult{
		QueryID:     qresp.ID,
		Finished:    qresp.NextURI == "",
		NextURI:     qresp.NextURI,
		UpdateType:  qresp.UpdateType,
		UpdateCount: qresp.UpdateCount,
	}

	// Parse columns if available
	if len(qresp.Columns) > 0 {
		// Initialize driverRows to handle type parsing
		rows := &driverRows{}
		err := rows.initColumns(qresp)
		if err != nil {
			return nil, fmt.Errorf("failed to parse columns: %w", err)
		}
		rows.data = qresp.Data
		rows.rowindex = 0

		columns := rows.columns

		// Extract column types
		columnTypes := rowsColumnInfoSetupConnLocked(rows)

		// Convert data rows
		data := make([][]interface{}, len(qresp.Data))
		for i := range qresp.Data {
			dest := make([]driver.Value, len(rows.coltype))
			rows.rowindex = i
			err := rows.Next(dest)
			if err != nil {
				return nil, fmt.Errorf("failed to parse row %d: %w", i, err)
			}

			// Convert driver.Value to interface{}
			data[i] = make([]interface{}, len(dest))
			for j, val := range dest {
				data[i][j] = val
			}
		}

		// Create PollingRows with the parsed data
		result.Rows = newPollingRows(columns, columnTypes, data)
	}

	return result, nil
}

// PollingRows wraps query result data to provide a sql.Rows-like interface for easier
// iteration and scanning of query results.
type PollingRows struct {
	columns      []string
	columnTypes  []*PollingColumnType
	data         [][]interface{}
	nextRowIndex int
	lastErr      error
}

// newPollingRows creates a PollingRows from columns, column types, and data.
// This is an internal constructor used by buildPollingResult.
func newPollingRows(columns []string, columnTypes []*PollingColumnType, data [][]interface{}) *PollingRows {
	return &PollingRows{
		columns:      columns,
		columnTypes:  columnTypes,
		data:         data,
		nextRowIndex: -1,
	}
}

// Columns returns the column names.
func (pr *PollingRows) Columns() ([]string, error) {
	return pr.columns, nil
}

// ColumnTypes returns the column type information.
func (pr *PollingRows) ColumnTypes() ([]*PollingColumnType, error) {
	return pr.columnTypes, nil
}

// Next advances to the next row. Returns false when there are no more rows.
func (pr *PollingRows) Next() bool {
	pr.nextRowIndex++
	return pr.nextRowIndex < len(pr.data)
}

// Scan copies the columns in the current row into the values pointed at by dest.
func (pr *PollingRows) Scan(dest ...any) error {
	if pr.nextRowIndex < 0 || pr.nextRowIndex >= len(pr.data) {
		pr.lastErr = io.EOF
		return pr.lastErr
	}

	row := pr.data[pr.nextRowIndex]
	if len(dest) != len(row) {
		pr.lastErr = fmt.Errorf("trino: expected %d destination arguments in Scan, not %d", len(row), len(dest))
		return pr.lastErr
	}

	for i, val := range row {
		pr.lastErr = convertAssign(dest[i], val)
		if pr.lastErr != nil {
			pr.lastErr = fmt.Errorf(`trino: Scan error on column index %d, name %q: %w`, i, pr.columns[i], pr.lastErr)
			return pr.lastErr
		}
	}
	return nil
}

// Close closes the rows iterator. Currently a no-op.
func (pr *PollingRows) Close() error {
	return nil
}

// Err returns the error, if any, that was encountered during iteration.
func (pr *PollingRows) Err() error {
	if pr.lastErr != io.EOF {
		return pr.lastErr
	}
	return nil
}

// Copy of database/sql.rowsColumnInfoSetupConnLocked, writing into PollingColumnType
// instead of database/sql.ColumnType.
func rowsColumnInfoSetupConnLocked(rowsi driver.Rows) []*PollingColumnType {
	names := rowsi.Columns()

	list := make([]*PollingColumnType, len(names))
	for i := range list {
		ci := &PollingColumnType{name: names[i]}
		list[i] = ci

		if prop, ok := rowsi.(driver.RowsColumnTypeScanType); ok {
			ci.scanType = prop.ColumnTypeScanType(i)
		} else {
			ci.scanType = reflect.TypeFor[any]()
		}
		if prop, ok := rowsi.(driver.RowsColumnTypeDatabaseTypeName); ok {
			ci.databaseType = prop.ColumnTypeDatabaseTypeName(i)
		}
		if prop, ok := rowsi.(driver.RowsColumnTypeLength); ok {
			ci.length, ci.hasLength = prop.ColumnTypeLength(i)
		}
		if prop, ok := rowsi.(driver.RowsColumnTypeNullable); ok {
			ci.nullable, ci.hasNullable = prop.ColumnTypeNullable(i)
		}
		if prop, ok := rowsi.(driver.RowsColumnTypePrecisionScale); ok {
			ci.precision, ci.scale, ci.hasPrecisionScale = prop.ColumnTypePrecisionScale(i)
		}
	}
	return list
}

// convertAssign is linked to database/sql.convertAssign which is used to convert
// interface{} values from the driver to typed destinations.
//
// This method should be stable across versions as it is widely accessed across packages using
// linkname, and documented as such:
// https://cs.opensource.google/go/go/+/master:src/database/sql/convert.go;l=209-223
//
//go:linkname convertAssign database/sql.convertAssign
func convertAssign(dest, src any) error

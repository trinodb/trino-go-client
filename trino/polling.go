package trino

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "unsafe" // for go:linkname
)

const (
	pollingUserName = "trino-polling-user"
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
	// this poll, if any (null when there was no data to return).
	Rows PollingRows

	// Partial results of DML statements (INSERT, UPDATE, DELETE) returned in
	// this poll, if any.
	UpdateType  string
	UpdateCount int64
}

// PollingRows provides an interface for iterating over query result rows.
// Both direct protocol (directPollingRows) and spooling protocol (spoolingPollingRows)
// implement this interface.
type PollingRows interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
	ColumnTypes() ([]*PollingColumnType, error)
	Close() error
	Err() error
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

	return pc.buildPollingResult(ctx, qresp)
}

// PollQuery polls for more query results using the NextURI from a previous
// result.
func (pc *PollingConn) PollQuery(ctx context.Context, nextURI string) (*PollingResult, error) {
	st := &driverStmt{conn: pc.conn, query: ""}

	qresp, err := pc.pollQuery(ctx, st, nextURI)
	if err != nil {
		return nil, err
	}

	return pc.buildPollingResult(ctx, qresp)
}

// CancelQuery cancels an in-progress query by sending a DELETE request to the
// nextURI. This is safe to call even after a query has completed.
func (pc *PollingConn) CancelQuery(ctx context.Context, nextURI string) error {
	hs := http.Header{trinoUserHeader: {pollingUserName}}

	if nextURI == "" {
		return errors.New("trino: cannot cancel query with empty nextURI")
	}

	req, err := pc.conn.newRequest(ctx, "DELETE", nextURI, nil, hs)
	if err != nil {
		return err
	}

	resp, err := pc.conn.roundTrip(ctx, req)
	if err != nil {
		// If the error is StatusNoContent, the query was successfully cancelled
		qferr, ok := err.(*ErrQueryFailed)
		if ok && qferr.StatusCode == http.StatusNoContent {
			return nil
		}
		return err
	}
	resp.Body.Close()
	return nil
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

			if st.conn.forwardAuthorizationHeader && arg.Name == accessTokenConfig {
				token := arg.Value.(string)
				hs.Add(authorizationHeader, getAuthorization(token))
				continue
			}

			if arg.Name == trinoEncoding {
				hs.Add(trinoQueryDataEncodingHeader, arg.Value.(string))
				continue
			}

			if arg.Name == trinoSpoolingWorkerCount {
				numberOfWorkers, err := strconv.Atoi(arg.Value.(string))
				if err != nil {
					return nil, err
				}
				st.spoolingWorkerCount = numberOfWorkers
				continue
			}

			if arg.Name == trinoMaxOutOfOrdersSegments {
				maxSegmentsOutOfOrder, err := strconv.Atoi(arg.Value.(string))
				if err != nil {
					return nil, err
				}
				st.spoolingMaxOutOfOrderSegments = maxSegmentsOutOfOrder
				continue
			}

			if strings.HasPrefix(arg.Name, trinoHeaderPrefix) {
				headerValue, err := formatHeaderValue(arg.Name, arg.Value)
				if err != nil {
					return nil, err
				}

				if arg.Name == trinoUserHeader {
					st.user = headerValue
				}

				if arg.Name == trinoRoleHeader {
					st.conn.httpHeaders.Set(trinoRoleHeader, headerValue)
				}

				hs.Add(arg.Name, headerValue)
			} else {
				s, err := Serial(arg.Value)
				if err != nil {
					return nil, err
				}

				if st.conn.useExplicitPrepare && hs.Get(preparedStatementHeader) == "" {
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
			if st.conn.useExplicitPrepare {
				query = "EXECUTE " + preparedStatementName + " USING " + strings.Join(ss, ", ")
			} else {
				query = "EXECUTE IMMEDIATE " + formatStringLiteral(st.query) + " USING " + strings.Join(ss, ", ")
			}
		}
	}

	if st.spoolingWorkerCount > st.spoolingMaxOutOfOrderSegments {
		return nil, fmt.Errorf("spooling worker cannot be greater than max out of order segments allowed. spooling workers: %d, allowed out of order segments: %d", st.spoolingWorkerCount, st.spoolingMaxOutOfOrderSegments)
	}

	if hs.Get(trinoQueryDataEncodingHeader) == "" {
		hs.Add(trinoQueryDataEncodingHeader, defaulttrinoEncoding)
	}

	var cancel context.CancelFunc = func() {}
	if st.conn.queryTimeout != nil {
		ctx, cancel = context.WithTimeout(ctx, *st.conn.queryTimeout)
	} else if _, ok := ctx.Deadline(); !ok {
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
	hs := http.Header{trinoUserHeader: {pollingUserName}}

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
func (pc *PollingConn) buildPollingResult(ctx context.Context, qresp *queryResponse) (*PollingResult, error) {
	result := &PollingResult{
		QueryID:     qresp.ID,
		Finished:    qresp.NextURI == "",
		NextURI:     qresp.NextURI,
		UpdateType:  qresp.UpdateType,
		UpdateCount: qresp.UpdateCount,
	}

	// Parse columns if available
	if len(qresp.Columns) > 0 {
		// Check the protocol type based on qresp.Data type
		switch data := qresp.Data.(type) {
		case []interface{}:
			// Direct protocol
			rows, err := newDirectPollingRows(qresp, data)
			if err != nil {
				return nil, err
			}
			result.Rows = rows

		case map[string]interface{}:
			// Spooling protocol
			rows, err := newSpoolingPollingRows(pc, ctx, qresp, data)
			if err != nil {
				return nil, err
			}
			result.Rows = rows

		case nil:
			// No data, this is fine
			result.Rows = nil

		default:
			return nil, fmt.Errorf("unexpected data type: expected []interface{} (direct protocol) or map[string]interface{} (spooling protocol), got %T", qresp.Data)
		}
	}

	return result, nil
}

// directPollingRows wraps query result data to provide a sql.Rows-like interface for easier
// iteration and scanning of query results from the direct protocol.
type directPollingRows struct {
	columns      []string
	columnTypes  []*PollingColumnType
	data         [][]interface{}
	nextRowIndex int
	lastErr      error
}

// newDirectPollingRowsFromResponse creates a directPollingRows by parsing the direct protocol
// data from a query response.
func newDirectPollingRows(qresp *queryResponse, data []interface{}) (*directPollingRows, error) {
	// Initialize driverRows to handle type parsing
	rows := &driverRows{stmt: &driverStmt{usingSpooledProtocol: false}}
	err := rows.initColumns(qresp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse columns: %w", err)
	}

	columns := rows.columns
	columnTypes := rowsColumnInfoSetupConnLocked(rows)

	// Convert data rows from direct protocol format
	rowsData := make([][]interface{}, len(data))
	for i, item := range data {
		if row, ok := item.([]interface{}); ok {
			dest := make([]driver.Value, len(rows.coltype))
			rows.data = []queryData{row}
			rows.rowindex = 0
			err := rows.Next(dest)
			if err != nil {
				return nil, fmt.Errorf("failed to parse row %d: %w", i, err)
			}

			// Convert driver.Value to interface{}
			rowsData[i] = make([]interface{}, len(dest))
			for j, val := range dest {
				rowsData[i][j] = val
			}
		} else {
			return nil, fmt.Errorf("unexpected data type for row at index %d: expected []interface{}, got %T", i, item)
		}
	}

	return &directPollingRows{
		columns:      columns,
		columnTypes:  columnTypes,
		data:         rowsData,
		nextRowIndex: -1,
	}, nil
}

// Columns returns the column names.
func (pr *directPollingRows) Columns() ([]string, error) {
	return pr.columns, nil
}

// ColumnTypes returns the column type information.
func (pr *directPollingRows) ColumnTypes() ([]*PollingColumnType, error) {
	return pr.columnTypes, nil
}

// Next advances to the next row. Returns false when there are no more rows.
func (pr *directPollingRows) Next() bool {
	pr.nextRowIndex++
	return pr.nextRowIndex < len(pr.data)
}

// Scan copies the columns in the current row into the values pointed at by dest.
func (pr *directPollingRows) Scan(dest ...any) error {
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
func (pr *directPollingRows) Close() error {
	return nil
}

// Err returns the error, if any, that was encountered during iteration.
func (pr *directPollingRows) Err() error {
	if pr.lastErr != io.EOF {
		return pr.lastErr
	}
	return nil
}

// spoolingPollingRows wraps spooled query result data and downloads segments
// lazily on-demand as Next() is called. This is a simple, sequential
// implementation that downloads one segment at a time as needed.
type spoolingPollingRows struct {
	conn        *PollingConn
	ctx         context.Context
	columns     []string
	columnTypes []*PollingColumnType
	driverRows  *driverRows
	lastErr     error
	closed      bool

	// Segment metadata
	segments []spooledMetadata
	encoding string

	// Current segment being consumed
	currentSegmentData  [][]interface{}
	currentRowInSegment int
	nextSegmentIndex    int
}

// newSpoolingPollingRows creates a spoolingPollingRows by parsing the spooling
// protocol data from a query response.
func newSpoolingPollingRows(
	pc *PollingConn,
	ctx context.Context,
	qresp *queryResponse,
	data map[string]interface{},
) (*spoolingPollingRows, error) {
	// Parse encoding
	encoding, ok := data["encoding"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid or missing 'encoding' field on spooling protocol, expected string")
	}

	// Parse segments array
	segmentsRaw, ok := data["segments"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid or missing 'segments' field on spooling protocol, expected []interface{}")
	}

	// Initialize driverRows to handle column type parsing
	rows := &driverRows{stmt: &driverStmt{usingSpooledProtocol: true}}
	err := rows.initColumns(qresp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse columns: %w", err)
	}

	columns := rows.columns
	columnTypes := rowsColumnInfoSetupConnLocked(rows)

	// Parse all segment metadata (but don't download yet)
	segments := make([]spooledMetadata, len(segmentsRaw))
	for i, seg := range segmentsRaw {
		segMap, ok := seg.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("segment at index %d is invalid: expected map[string]interface{}, got %T", i, seg)
		}

		// Parse segment metadata (row counts, sizes, etc.)
		metadataRaw, ok := segMap["metadata"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("segment at index %d missing metadata", i)
		}

		segMeta, err := parseSegmentMetadata(metadataRaw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse segment %d metadata: %w", i, err)
		}

		// Handle both inline and spooled segment types
		segmentType, _ := segMap["type"].(string)
		switch segmentType {
		case "inline":
			// For inline segments, we store the data field directly in the spooledMetadata
			// The uri field will be empty, which signals we should use inline data
			segments[i] = spooledMetadata{
				uri:      "", // Empty URI indicates inline data
				encoding: encoding,
				metadata: segMeta,
			}
			// Store the base64 data in the headers map for later retrieval
			if data, ok := segMap["data"].(string); ok {
				segments[i].headers = map[string]interface{}{"data": data}
			} else {
				return nil, fmt.Errorf("segment at index %d has type 'inline' but missing or invalid 'data' field", i)
			}

		case "spooled":
			// Parse full spooled metadata (URI, headers, etc.)
			segments[i], err = parseSpooledMetadata(segMap, i, segMeta, encoding)
			if err != nil {
				return nil, fmt.Errorf("failed to parse segment %d: %w", i, err)
			}

		default:
			return nil, fmt.Errorf("segment at index %d has unknown or missing 'type' field: %q", i, segmentType)
		}
	}

	return &spoolingPollingRows{
		conn:                pc,
		ctx:                 ctx,
		segments:            segments,
		encoding:            encoding,
		columns:             columns,
		columnTypes:         columnTypes,
		driverRows:          rows,
		currentRowInSegment: 0,
		nextSegmentIndex:    0,
	}, nil
}

// Next advances to the next row. Returns false when there are no more rows.
// Automatically downloads the next segment when the current segment is exhausted.
func (spr *spoolingPollingRows) Next() bool {
	if spr.closed {
		return false
	}

	// Check if we need to fetch the next segment
	if spr.currentRowInSegment >= len(spr.currentSegmentData) {
		if !spr.fetchNextSegment() {
			return false
		}
	}

	spr.currentRowInSegment++
	return spr.currentRowInSegment <= len(spr.currentSegmentData)
}

// fetchNextSegment downloads and decodes the next segment.
// Returns false if there are no more segments or an error occurred.
func (spr *spoolingPollingRows) fetchNextSegment() bool {
	if spr.nextSegmentIndex >= len(spr.segments) {
		return false // All segments consumed
	}

	segment := spr.segments[spr.nextSegmentIndex]

	var rawData []byte
	var err error

	// Handle inline vs spooled segments
	if segment.uri == "" {
		// Inline segment - decode base64 data directly
		if data, ok := segment.headers["data"].(string); ok {
			rawData, err = base64.StdEncoding.DecodeString(data)
			if err != nil {
				spr.lastErr = fmt.Errorf("failed to decode base64 data for inline segment %d: %w", spr.nextSegmentIndex, err)
				return false
			}
		} else {
			spr.lastErr = fmt.Errorf("inline segment %d missing data field", spr.nextSegmentIndex)
			return false
		}
	} else {
		// Spooled segment - download from URI
		fetcher := &SegmentFetcher{
			ctx:             spr.ctx,
			httpClient:      spr.conn.conn.httpClient,
			spooledMetadata: segment,
		}

		rawData, err = fetcher.fetchSegment()
		if err != nil {
			spr.lastErr = fmt.Errorf("failed to fetch segment %d: %w", spr.nextSegmentIndex, err)
			return false
		}
	}

	// Decode segment using existing decodeSegment function
	rawSegmentData, err := decodeSegment(rawData, spr.encoding, segment.metadata)
	if err != nil {
		spr.lastErr = fmt.Errorf("failed to decode segment %d: %w", spr.nextSegmentIndex, err)
		return false
	}

	// Convert raw JSON data to proper types
	spr.currentSegmentData = make([][]interface{}, len(rawSegmentData))

	for i, rawRow := range rawSegmentData {
		dest := make([]driver.Value, len(spr.driverRows.coltype))
		spr.driverRows.data = []queryData{rawRow}
		spr.driverRows.rowindex = 0
		err := spr.driverRows.Next(dest)
		if err != nil {
			spr.lastErr = fmt.Errorf("failed to convert row %d in segment %d: %w", i, spr.nextSegmentIndex, err)
			return false
		}

		// Convert driver.Value to interface{}
		spr.currentSegmentData[i] = make([]interface{}, len(dest))
		for j, val := range dest {
			spr.currentSegmentData[i][j] = val
		}
	}

	// Reset row index for new segment
	spr.currentRowInSegment = 0
	spr.nextSegmentIndex++
	return len(spr.currentSegmentData) > 0
}

// Scan copies the columns in the current row into the values pointed at by dest.
func (spr *spoolingPollingRows) Scan(dest ...any) error {
	if spr.closed {
		return io.EOF
	}

	if spr.currentRowInSegment < 1 || spr.currentRowInSegment > len(spr.currentSegmentData) {
		spr.lastErr = io.EOF
		return spr.lastErr
	}

	// Get the current row (1-indexed because Next() increments before returning)
	row := spr.currentSegmentData[spr.currentRowInSegment-1]

	if len(dest) != len(row) {
		spr.lastErr = fmt.Errorf("trino: expected %d destination arguments in Scan, not %d", len(row), len(dest))
		return spr.lastErr
	}

	// Convert each value to the appropriate type
	for i, val := range row {
		spr.lastErr = convertAssign(dest[i], val)
		if spr.lastErr != nil {
			spr.lastErr = fmt.Errorf(`trino: Scan error on column index %d, name %q: %w`, i, spr.columns[i], spr.lastErr)
			return spr.lastErr
		}
	}
	return nil
}

// Columns returns the column names.
func (spr *spoolingPollingRows) Columns() ([]string, error) {
	return spr.columns, nil
}

// ColumnTypes returns the column type information.
func (spr *spoolingPollingRows) ColumnTypes() ([]*PollingColumnType, error) {
	return spr.columnTypes, nil
}

// Close closes the rows iterator. Marks the iterator as closed to prevent further use.
func (spr *spoolingPollingRows) Close() error {
	spr.closed = true
	spr.currentSegmentData = nil
	return nil
}

// Err returns the error, if any, that was encountered during iteration.
func (spr *spoolingPollingRows) Err() error {
	if spr.lastErr != io.EOF {
		return spr.lastErr
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

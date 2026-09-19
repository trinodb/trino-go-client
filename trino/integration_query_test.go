package trino

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// integration tests based on python tests:
// https://github.com/trinodb/trino-python-client/tree/master/integration_tests

type nodesRow struct {
	NodeID      string
	HTTPURI     string
	NodeVersion string
	Coordinator bool
	State       string
}

func TestIntegrationSelectQueryIterator(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	rows, err := db.Query("SELECT * FROM system.runtime.nodes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var col nodesRow
		err = rows.Scan(
			&col.NodeID,
			&col.HTTPURI,
			&col.NodeVersion,
			&col.Coordinator,
			&col.State,
		)
		if err != nil {
			t.Fatal(err)
		}
		if col.NodeID != "test" {
			t.Errorf("Expected node_id == test but got %s", col.NodeID)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count < 1 {
		t.Error("no rows returned")
	}
}

func TestIntegrationSelectQueryNoResult(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	row := db.QueryRow("SELECT * FROM system.runtime.nodes where false")
	var col nodesRow
	err := row.Scan(
		&col.NodeID,
		&col.HTTPURI,
		&col.NodeVersion,
		&col.Coordinator,
		&col.State,
	)
	if err == nil {
		t.Fatalf("unexpected query returning data: %+v", col)
	}
}

func TestIntegrationSelectFailedQuery(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	rows, err := db.Query("SELECT * FROM catalog.schema.do_not_exist")
	if err == nil {
		rows.Close()
		t.Fatal("query to invalid catalog succeeded")
	}
	queryFailed, ok := err.(*ErrQueryFailed)
	if !ok {
		t.Fatal("unexpected error:", err)
	}
	trinoErr, ok := errors.Unwrap(queryFailed).(*ErrTrino)
	if !ok {
		t.Fatal("unexpected error:", trinoErr)
	}
	expected := ErrTrino{
		Message:   "line 1:15: Catalog 'catalog'",
		SqlState:  "",
		ErrorCode: 44,
		ErrorName: "CATALOG_NOT_FOUND",
		ErrorType: "USER_ERROR",
		ErrorLocation: ErrorLocation{
			LineNumber:   1,
			ColumnNumber: 15,
		},
		FailureInfo: FailureInfo{
			Type:    "io.trino.spi.TrinoException",
			Message: "line 1:15: Catalog 'catalog'",
		},
	}
	if !strings.HasPrefix(trinoErr.Message, expected.Message) {
		t.Fatalf("expected ErrTrino.Message to start with `%s`, got: %s", expected.Message, trinoErr.Message)
	}
	if trinoErr.SqlState != expected.SqlState {
		t.Fatalf("expected ErrTrino.SqlState to be `%s`, got: %s", expected.SqlState, trinoErr.SqlState)
	}
	if trinoErr.ErrorCode != expected.ErrorCode {
		t.Fatalf("expected ErrTrino.ErrorCode to be `%d`, got: %d", expected.ErrorCode, trinoErr.ErrorCode)
	}
	if trinoErr.ErrorName != expected.ErrorName {
		t.Fatalf("expected ErrTrino.ErrorName to be `%s`, got: %s", expected.ErrorName, trinoErr.ErrorName)
	}
	if trinoErr.ErrorType != expected.ErrorType {
		t.Fatalf("expected ErrTrino.ErrorType to be `%s`, got: %s", expected.ErrorType, trinoErr.ErrorType)
	}
	if trinoErr.ErrorLocation.LineNumber != expected.ErrorLocation.LineNumber {
		t.Fatalf("expected ErrTrino.ErrorLocation.LineNumber to be `%d`, got: %d", expected.ErrorLocation.LineNumber, trinoErr.ErrorLocation.LineNumber)
	}
	if trinoErr.ErrorLocation.ColumnNumber != expected.ErrorLocation.ColumnNumber {
		t.Fatalf("expected ErrTrino.ErrorLocation.ColumnNumber to be `%d`, got: %d", expected.ErrorLocation.ColumnNumber, trinoErr.ErrorLocation.ColumnNumber)
	}
	if trinoErr.FailureInfo.Type != expected.FailureInfo.Type {
		t.Fatalf("expected ErrTrino.FailureInfo.Type to be `%s`, got: %s", expected.FailureInfo.Type, trinoErr.FailureInfo.Type)
	}
	if !strings.HasPrefix(trinoErr.FailureInfo.Message, expected.FailureInfo.Message) {
		t.Fatalf("expected ErrTrino.FailureInfo.Message to start with `%s`, got: %s", expected.FailureInfo.Message, trinoErr.FailureInfo.Message)
	}
}

type tpchRow struct {
	CustKey    int
	Name       string
	Address    string
	NationKey  int
	Phone      string
	AcctBal    float64
	MktSegment string
	Comment    string
}

func TestIntegrationSelectTpch1000(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	rows, err := db.Query("SELECT * FROM tpch.sf1.customer LIMIT 1000")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var col tpchRow
		err = rows.Scan(
			&col.CustKey,
			&col.Name,
			&col.Address,
			&col.NationKey,
			&col.Phone,
			&col.AcctBal,
			&col.MktSegment,
			&col.Comment,
		)
		if err != nil {
			t.Fatal(err)
		}
		/*
			if col.CustKey == 1 && col.AcctBal != 711.56 {
				t.Fatal("unexpected acctbal for custkey=1:", col.AcctBal)
			}
		*/
	}
	if rows.Err() != nil {
		t.Fatal(err)
	}
	if count != 1000 {
		t.Fatal("not enough rows returned:", count)
	}
}

func TestIntegrationSelectCancelQuery(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	deadline := time.Now().Add(200 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT * FROM tpch.sf1.customer")
	if err != nil {
		goto handleErr
	}
	defer rows.Close()
	for rows.Next() {
		var col tpchRow
		err = rows.Scan(
			&col.CustKey,
			&col.Name,
			&col.Address,
			&col.NationKey,
			&col.Phone,
			&col.AcctBal,
			&col.MktSegment,
			&col.Comment,
		)
		if err != nil {
			break
		}
	}
	if err = rows.Err(); err == nil {
		t.Fatal("unexpected query with deadline succeeded")
	}
handleErr:
	errmsg := err.Error()
	for _, msg := range []string{"cancel", "deadline"} {
		if strings.Contains(errmsg, msg) {
			return
		}
	}
	t.Fatal("unexpected error:", err)
}

func TestIntegrationSessionProperties(t *testing.T) {
	dsn := *integrationServerFlag
	dsn += "?session_properties=query_max_run_time%3A10m%3Bquery_priority%3A2"
	db := integrationOpen(t, dsn)
	defer db.Close()
	rows, err := db.Query("SHOW SESSION")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		col := struct {
			Name        string
			Value       string
			Default     string
			Type        string
			Description string
		}{}
		err = rows.Scan(
			&col.Name,
			&col.Value,
			&col.Default,
			&col.Type,
			&col.Description,
		)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case col.Name == "query_max_run_time" && col.Value != "10m":
			t.Fatal("unexpected value for query_max_run_time:", col.Value)
		case col.Name == "query_priority" && col.Value != "2":
			t.Fatal("unexpected value for query_priority:", col.Value)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationNoResults(t *testing.T) {
	db := integrationOpen(t)
	rows, err := db.Query("SELECT 1 LIMIT 0")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		t.Fatal(errors.New("Rows returned"))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationQueryParametersSelect(t *testing.T) {
	scenarios := []struct {
		name          string
		query         string
		args          []interface{}
		expectedError error
		expectedRows  int
	}{
		{
			name:         "valid string as varchar",
			query:        "SELECT * FROM system.runtime.nodes WHERE system.runtime.nodes.node_id=?",
			args:         []interface{}{"test"},
			expectedRows: 1,
		},
		{
			name:         "valid int as bigint",
			query:        "SELECT * FROM tpch.sf1.customer WHERE custkey=? LIMIT 2",
			args:         []interface{}{int(1)},
			expectedRows: 1,
		},
		{
			name:          "invalid string as bigint",
			query:         "SELECT * FROM tpch.sf1.customer WHERE custkey=? LIMIT 2",
			args:          []interface{}{"1"},
			expectedError: errors.New(`trino: query failed (200 OK): "USER_ERROR: line 1:46: Cannot apply operator: bigint = varchar(1)"`),
		},
		{
			name:          "valid string as date",
			query:         "SELECT * FROM tpch.sf1.lineitem WHERE shipdate=? LIMIT 2",
			args:          []interface{}{"1995-01-27"},
			expectedError: errors.New(`trino: query failed (200 OK): "USER_ERROR: line 1:47: Cannot apply operator: date = varchar(10)"`),
		},
	}

	for i := range scenarios {
		scenario := scenarios[i]

		t.Run(scenario.name, func(t *testing.T) {
			db := integrationOpen(t)
			defer db.Close()

			rows, err := db.Query(scenario.query, scenario.args...)
			if err != nil {
				if scenario.expectedError == nil {
					t.Errorf("Unexpected err: %s", err)
					return
				}
				if err.Error() == scenario.expectedError.Error() {
					return
				}
				t.Errorf("Expected err to be %s but got %s", scenario.expectedError, err)
			}

			if scenario.expectedError != nil {
				t.Error("missing expected error")
				return
			}

			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			if count != scenario.expectedRows {
				t.Errorf("expecting %d rows, got %d", scenario.expectedRows, count)
			}
		})
	}
}

func TestIntegrationQueryNextAfterClose(t *testing.T) {
	// NOTE: This is testing invalid behaviour. It ensures that we don't
	// panic if we call driverRows.Next after we closed the driverStmt.

	ctx := context.Background()
	conn, err := (&Driver{}).Open(*integrationServerFlag)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.(driver.ConnPrepareContext).PrepareContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("Failed preparing query: %v", err)
	}

	rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, []driver.NamedValue{})
	if err != nil {
		t.Fatalf("Failed running query: %v", err)
	}
	defer rows.Close()

	stmt.Close() // NOTE: the important bit.

	var result driver.Value
	if err := rows.Next([]driver.Value{result}); err != nil && !spoolingProtocolSupported {
		t.Fatalf("unexpected result: %+v, no error was expected", err)
	}
	if err := rows.Next([]driver.Value{result}); err != io.EOF {
		t.Fatalf("unexpected result: %+v, expected io.EOF", err)
	}
}

func TestIntegrationExec(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()

	_, err := db.Query(`SELECT count(*) FROM nation`)
	expected := "Schema must be specified when session schema is not set"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Expected to fail to execute query with error: %v, got: %v", expected, err)
	}

	result, err := db.Exec("USE tpch.sf100")
	if err != nil {
		t.Fatal("Failed executing query:", err.Error())
	}
	if result == nil {
		t.Fatal("Expected exec result to be not nil")
	}

	a, err := result.RowsAffected()
	if err != nil {
		t.Fatal("Expected RowsAffected not to return any error, got:", err)
	}
	if a != 0 {
		t.Fatal("Expected RowsAffected to be zero, got:", a)
	}
	rows, err := db.Query(`SELECT count(*) FROM nation`)
	if err != nil {
		t.Fatal("Failed executing query:", err.Error())
	}
	if rows == nil || !rows.Next() {
		t.Fatal("Failed fetching results")
	}
}

func TestIntegrationUnsupportedHeader(t *testing.T) {
	dsn := *integrationServerFlag
	dsn += "?catalog=tpch&schema=sf10"
	db := integrationOpen(t, dsn)
	defer db.Close()
	cases := []struct {
		query string
		err   error
	}{
		{
			query: "SET ROLE dummy",
			err:   errors.New(`trino: query failed (200 OK): "USER_ERROR: line 1:1: Role 'dummy' does not exist"`),
		},
		{
			query: "SET PATH dummy",
			err:   errors.New(`trino: query failed (200 OK): "USER_ERROR: SET PATH not supported by client"`),
		},
	}
	for _, c := range cases {
		_, err := db.Query(c.query)
		if err == nil || err.Error() != c.err.Error() {
			t.Fatal("unexpected error:", err)
		}
	}
}

func TestIntegrationQueryContext(t *testing.T) {
	tests := []struct {
		name           string
		timeout        time.Duration
		expectedErrMsg string
	}{
		{
			name:           "Context Cancellation",
			timeout:        0,
			expectedErrMsg: "canceled",
		},
		{
			name:           "Context Deadline Exceeded",
			timeout:        3 * time.Second,
			expectedErrMsg: "context deadline exceeded",
		},
	}

	if err := RegisterCustomClient("uncompressed", &http.Client{Transport: &http.Transport{DisableCompression: true}}); err != nil {
		t.Fatal(err)
	}

	dsn := *integrationServerFlag + "?catalog=tpch&schema=sf100&source=cancel-test&custom_client=uncompressed"
	db := integrationOpen(t, dsn)
	defer db.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ctx context.Context
			var cancel context.CancelFunc

			if tt.timeout == 0 {
				ctx, cancel = context.WithCancel(context.Background())
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), tt.timeout)
			}
			defer cancel()

			errCh := make(chan error, 1)
			done := make(chan struct{})
			longQuery := "SELECT COUNT(*) FROM lineitem"

			go func() {
				// query will complete in ~7s unless cancelled
				rows, err := db.QueryContext(ctx, longQuery)
				if err != nil {
					errCh <- err
					return
				}
				defer rows.Close()

				rows.Next()
				if err = rows.Err(); err != nil {
					errCh <- err
					return
				}
				close(done)
			}()

			// Poll system.runtime.queries to get the query ID
			var queryID string
			pollCtx, pollCancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer pollCancel()

			for {
				row := db.QueryRowContext(pollCtx, "SELECT query_id FROM system.runtime.queries WHERE state = 'RUNNING' AND source = 'cancel-test' AND query = ?", longQuery)
				err := row.Scan(&queryID)
				if err == nil {
					break
				}
				if err != sql.ErrNoRows {
					t.Fatal("failed to read query ID:", err)
				}
				if err = contextSleep(pollCtx, 100*time.Millisecond); err != nil {
					t.Fatal("query did not start in 1 second")
				}
			}

			if tt.timeout == 0 {
				cancel()
			}

			// Wait for the query to be canceled or completed
			select {
			case <-done:
				t.Fatal("unexpected query succeeded despite cancellation or deadline")
			case err := <-errCh:
				if !strings.Contains(err.Error(), tt.expectedErrMsg) {
					t.Fatalf("expected error containing %q, but got: %v", tt.expectedErrMsg, err)
				}
			}

			// Poll system.runtime.queries to verify the query was canceled
			pollCtx, pollCancel = context.WithTimeout(context.Background(), 2*time.Second)
			defer pollCancel()

			for {
				row := db.QueryRowContext(pollCtx, "SELECT state, error_code FROM system.runtime.queries WHERE query_id = ?", queryID)
				var state string
				var code *string
				err := row.Scan(&state, &code)
				if err != nil {
					t.Fatal("failed to read query state:", err)
				}
				if state == "FAILED" && code != nil && *code == "USER_CANCELED" {
					return
				}
				if err = contextSleep(pollCtx, 100*time.Millisecond); err != nil {
					t.Fatalf("query was not canceled in 2 seconds; state: %s, code: %v, err: %v", state, code, err)
				}
			}
		})
	}
}

func TestIntegrationLargeQuery(t *testing.T) {
	version, err := strconv.Atoi(*trinoImageTagFlag)
	if (err != nil && *trinoImageTagFlag != "latest") || (err == nil && version < 418) {
		t.Skip("Skipping test when not using Trino 418 or later.")
	}
	dsn := *integrationServerFlag
	dsn += "?explicitPrepare=false"
	db := integrationOpen(t, dsn)
	defer db.Close()
	rows, err := db.Query("SELECT ?, '"+strings.Repeat("a", 5000000)+"'", 42)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if rows.Err() != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("not enough rows returned:", count)
	}
}

func TestQueryForUsername(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	c := &Config{
		ServerURI:         *integrationServerFlag,
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db, err := sql.Open("trino", dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	rows, err := db.Query("SELECT current_user", sql.Named("X-Trino-User", string("TestUser")))
	require.NoError(t, err, "Failed executing query")
	assert.NotNil(t, rows)

	for rows.Next() {
		var user string
		require.NoError(t, rows.Scan(&user), "Failed scanning query result")

		assert.Equal(t, "TestUser", user, "Expected value does not equal result value")
	}
}

type TestQueryProgressCallback struct {
	progressMap map[time.Time]float64
	statusMap   map[time.Time]string
}

func (qpc *TestQueryProgressCallback) Update(qpi QueryProgressInfo) {
	qpc.progressMap[time.Now()] = float64(qpi.QueryStats.ProgressPercentage)
	qpc.statusMap[time.Now()] = qpi.QueryStats.State
}

func TestQueryProgressWithCallback(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	c := &Config{
		ServerURI:         *integrationServerFlag,
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db, err := sql.Open("trino", dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	callback := &TestQueryProgressCallback{}

	_, err = db.Query("SELECT 2", sql.Named("X-Trino-Progress-Callback", callback))
	assert.EqualError(t, err, ErrInvalidProgressCallbackHeader.Error(), "unexpected error")
}

func TestQueryProgressWithCallbackPeriod(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	c := &Config{
		ServerURI:         *integrationServerFlag,
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db, err := sql.Open("trino", dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	progressMap := make(map[time.Time]float64)
	statusMap := make(map[time.Time]string)
	progressUpdater := &TestQueryProgressCallback{
		progressMap: progressMap,
		statusMap:   statusMap,
	}
	progressUpdaterPeriod, err := time.ParseDuration("1ms")
	require.NoError(t, err)

	rows, err := db.Query("SELECT 2",
		sql.Named("X-Trino-Progress-Callback", progressUpdater),
		sql.Named("X-Trino-Progress-Callback-Period", progressUpdaterPeriod),
	)
	require.NoError(t, err, "Failed executing query")
	assert.NotNil(t, rows)

	for rows.Next() {
		var ts string
		require.NoError(t, rows.Scan(&ts), "Failed scanning query result")

		assert.Equal(t, "2", ts, "Expected value does not equal result value")
	}

	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}

	// sort time in order to calculate interval
	assert.NotEmpty(t, progressMap)
	assert.NotEmpty(t, statusMap)
	var keys []time.Time
	for k := range statusMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Before(keys[j])
	})

	for i, k := range keys {
		if i > 0 {
			assert.GreaterOrEqual(t, k.Sub(keys[i-1]), progressUpdaterPeriod)
		}
		assert.GreaterOrEqual(t, progressMap[k], 0.0)
	}
}

func TestSession(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	err := RegisterCustomClient("uncompressed", &http.Client{Transport: &http.Transport{DisableCompression: true}})
	if err != nil {
		t.Fatal(err)
	}
	c := &Config{
		ServerURI:         *integrationServerFlag + "?custom_client=uncompressed",
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db, err := sql.Open("trino", dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Exec("SET SESSION join_distribution_type='BROADCAST'")
	require.NoError(t, err, "Failed executing query")

	row := db.QueryRow("SHOW SESSION LIKE 'join_distribution_type'")
	var name string
	var value string
	var defaultValue string
	var typeName string
	var description string
	err = row.Scan(&name, &value, &defaultValue, &typeName, &description)
	require.NoError(t, err, "Failed executing query")

	assert.Equal(t, "BROADCAST", value)

	_, err = db.Exec("RESET SESSION join_distribution_type")
	require.NoError(t, err, "Failed executing query")

	row = db.QueryRow("SHOW SESSION LIKE 'join_distribution_type'")
	err = row.Scan(&name, &value, &defaultValue, &typeName, &description)
	require.NoError(t, err, "Failed executing query")

	assert.Equal(t, "AUTOMATIC", value)
}

func TestExec(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	c := &Config{
		ServerURI:         *integrationServerFlag,
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db, err := sql.Open("trino", dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Exec("CREATE TABLE memory.default.test (id INTEGER, name VARCHAR, optional VARCHAR)")
	require.NoError(t, err, "Failed executing CREATE TABLE query")

	result, err := db.Exec("INSERT INTO memory.default.test (id, name, optional) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
		123, "abc", nil,
		456, "def", "present",
		789, "ghi", nil)
	require.NoError(t, err, "Failed executing INSERT query")
	_, err = result.LastInsertId()
	assert.Error(t, err, "trino: operation not supported")
	numRows, err := result.RowsAffected()
	require.NoError(t, err, "Failed checking rows affected")
	assert.Equal(t, numRows, int64(3))

	rows, err := db.Query("SELECT * FROM memory.default.test")
	require.NoError(t, err, "Failed executing DELETE query")

	expectedIds := []int{123, 456, 789}
	expectedNames := []string{"abc", "def", "ghi"}
	expectedOptionals := []sql.NullString{
		sql.NullString{Valid: false},
		sql.NullString{String: "present", Valid: true},
		sql.NullString{Valid: false},
	}
	actualIds := []int{}
	actualNames := []string{}
	actualOptionals := []sql.NullString{}
	for rows.Next() {
		var id int
		var name string
		var optional sql.NullString
		require.NoError(t, rows.Scan(&id, &name, &optional), "Failed scanning query result")
		actualIds = append(actualIds, id)
		actualNames = append(actualNames, name)
		actualOptionals = append(actualOptionals, optional)

	}
	assert.Equal(t, expectedIds, actualIds)
	assert.Equal(t, expectedNames, actualNames)
	assert.Equal(t, expectedOptionals, actualOptionals)

	_, err = db.Exec("DROP TABLE memory.default.test")
	require.NoError(t, err, "Failed executing DROP TABLE query")
}

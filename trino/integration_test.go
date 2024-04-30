// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trino

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"database/sql/driver"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	dt "github.com/ory/dockertest/v3"
)

var (
	pool     *dt.Pool
	resource *dt.Resource

	trinoImageTagFlag = flag.String(
		"trino_image_tag",
		os.Getenv("TRINO_IMAGE_TAG"),
		"Docker image tag used for the Trino server container",
	)
	integrationServerFlag = flag.String(
		"trino_server_dsn",
		os.Getenv("TRINO_SERVER_DSN"),
		"dsn of the Trino server used for integration tests instead of starting a Docker container",
	)
	integrationServerQueryTimeout = flag.Duration(
		"trino_query_timeout",
		5*time.Second,
		"max duration for Trino queries to run before giving up",
	)
	noCleanup = flag.Bool(
		"no_cleanup",
		false,
		"do not delete containers on exit",
	)
	tlsServer = ""
)

func TestMain(m *testing.M) {
	flag.Parse()
	DefaultQueryTimeout = *integrationServerQueryTimeout
	DefaultCancelQueryTimeout = *integrationServerQueryTimeout

	var err error
	if *integrationServerFlag == "" && !testing.Short() {
		pool, err = dt.NewPool("")
		if err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}
		pool.MaxWait = 1 * time.Minute

		wd, err := os.Getwd()
		if err != nil {
			log.Fatalf("Failed to get working directory: %s", err)
		}
		name := "trino-go-client-tests"
		var ok bool
		resource, ok = pool.ContainerByName(name)

		if !ok {
			err = generateCerts(wd + "/etc/secrets")
			if err != nil {
				log.Fatalf("Could not generate TLS certificates: %s", err)
			}
			if *trinoImageTagFlag == "" {
				*trinoImageTagFlag = "latest"
			}
			resource, err = pool.RunWithOptions(&dt.RunOptions{
				Name:       name,
				Repository: "trinodb/trino",
				Tag:        *trinoImageTagFlag,
				Mounts:     []string{wd + "/etc:/etc/trino"},
				ExposedPorts: []string{
					"8080/tcp",
					"8443/tcp",
				},
			})
			if err != nil {
				log.Fatalf("Could not start resource: %s", err)
			}
		}

		if err := pool.Retry(func() error {
			c, err := pool.Client.InspectContainer(resource.Container.ID)
			if err != nil {
				return err
			}
			if c.State.Health.Status != "healthy" {
				return errors.New("Not ready")
			}
			return nil
		}); err != nil {
			log.Fatalf("Timed out waiting for container to get ready: %s", err)
		}
		*integrationServerFlag = "http://test@localhost:" + resource.GetPort("8080/tcp")
		tlsServer = "https://test@localhost:" + resource.GetPort("8443/tcp")

		http.DefaultTransport.(*http.Transport).TLSClientConfig, err = getTLSConfig(wd + "/etc/secrets")
		if err != nil {
			log.Fatalf("Failed to set the default TLS config: %s", err)
		}
	}

	code := m.Run()

	if !*noCleanup && pool != nil && resource != nil {
		// You can't defer this because os.Exit doesn't care for defer
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	os.Exit(code)
}

func generateCerts(dir string) error {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate private key: %w", err)
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Trino Software Foundation"},
		},
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return fmt.Errorf("unable to marshal private key: %w", err)
	}
	privBlock := &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}
	err = writePEM(dir+"/private_key.pem", privBlock)
	if err != nil {
		return err
	}

	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return fmt.Errorf("unable to marshal public key: %w", err)
	}
	pubBlock := &pem.Block{Type: "PUBLIC KEY", Bytes: pubBytes}
	err = writePEM(dir+"/public_key.pem", pubBlock)
	if err != nil {
		return err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return fmt.Errorf("failed to create certificate: %w", err)
	}
	certBlock := &pem.Block{Type: "CERTIFICATE", Bytes: certBytes}
	err = writePEM(dir+"/certificate.pem", certBlock)
	if err != nil {
		return err
	}

	err = writePEM(dir+"/certificate_with_key.pem", certBlock, privBlock, pubBlock)
	if err != nil {
		return err
	}

	return nil
}

func writePEM(filename string, blocks ...*pem.Block) error {
	// all files are world-readable, so they can be read inside the Trino container
	out, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to open %s for writing: %w", filename, err)
	}
	for _, block := range blocks {
		if err := pem.Encode(out, block); err != nil {
			return fmt.Errorf("failed to write %s data to %s: %w", block.Type, filename, err)
		}
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("error closing %s: %w", filename, err)
	}
	return nil
}

func getTLSConfig(dir string) (*tls.Config, error) {
	certPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("failed to read the system cert pool: %s", err)
	}
	caCertPEM, err := os.ReadFile(dir + "/certificate.pem")
	if err != nil {
		return nil, fmt.Errorf("failed to read the certificate: %s", err)
	}
	ok := certPool.AppendCertsFromPEM(caCertPEM)
	if !ok {
		return nil, fmt.Errorf("failed to parse the certificate: %s", err)
	}
	return &tls.Config{
		RootCAs: certPool,
	}, nil
}

// integrationOpen opens a connection to the integration test server.
func integrationOpen(t *testing.T, dsn ...string) *sql.DB {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	target := *integrationServerFlag
	if len(dsn) > 0 {
		target = dsn[0]
	}
	db, err := sql.Open("trino", target)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

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
	dsn += "?session_properties=query_max_run_time=10m,query_priority=2"
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

func TestIntegrationTypeConversion(t *testing.T) {
	err := RegisterCustomClient("uncompressed", &http.Client{Transport: &http.Transport{DisableCompression: true}})
	if err != nil {
		t.Fatal(err)
	}
	dsn := *integrationServerFlag
	dsn += "?custom_client=uncompressed"
	db := integrationOpen(t, dsn)
	var (
		goTime            time.Time
		nullTime          NullTime
		goString          string
		nullString        sql.NullString
		nullStringSlice   NullSliceString
		nullStringSlice2  NullSlice2String
		nullStringSlice3  NullSlice3String
		nullInt64Slice    NullSliceInt64
		nullInt64Slice2   NullSlice2Int64
		nullInt64Slice3   NullSlice3Int64
		nullFloat64Slice  NullSliceFloat64
		nullFloat64Slice2 NullSlice2Float64
		nullFloat64Slice3 NullSlice3Float64
		goMap             map[string]interface{}
		nullMap           NullMap
		goRow             []interface{}
	)
	err = db.QueryRow(`
		SELECT
			TIMESTAMP '2017-07-10 01:02:03.004 UTC',
			CAST(NULL AS TIMESTAMP),
			CAST('string' AS VARCHAR),
			CAST(NULL AS VARCHAR),
			ARRAY['A', 'B', NULL],
			ARRAY[ARRAY['A'], NULL],
			ARRAY[ARRAY[ARRAY['A'], NULL], NULL],
			ARRAY[1, 2, NULL],
			ARRAY[ARRAY[1, 1, 1], NULL],
			ARRAY[ARRAY[ARRAY[1, 1, 1], NULL], NULL],
			ARRAY[1.0, 2.0, NULL],
			ARRAY[ARRAY[1.1, 1.1, 1.1], NULL],
			ARRAY[ARRAY[ARRAY[1.1, 1.1, 1.1], NULL], NULL],
			MAP(ARRAY['a', 'b'], ARRAY['c', 'd']),
			CAST(NULL AS MAP(ARRAY(INTEGER), ARRAY(INTEGER))),
			ROW(1, 'a', CAST('2017-07-10 01:02:03.004 UTC' AS TIMESTAMP(6) WITH TIME ZONE), ARRAY['c'])
	`).Scan(
		&goTime,
		&nullTime,
		&goString,
		&nullString,
		&nullStringSlice,
		&nullStringSlice2,
		&nullStringSlice3,
		&nullInt64Slice,
		&nullInt64Slice2,
		&nullInt64Slice3,
		&nullFloat64Slice,
		&nullFloat64Slice2,
		&nullFloat64Slice3,
		&goMap,
		&nullMap,
		&goRow,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationArgsConversion(t *testing.T) {
	dsn := *integrationServerFlag
	db := integrationOpen(t, dsn)
	value := 0
	err := db.QueryRow(`
		SELECT 1 FROM (VALUES (
			CAST(1 AS TINYINT),
			CAST(1 AS SMALLINT),
			CAST(1 AS INTEGER),
			CAST(1 AS BIGINT),
			CAST(1 AS REAL),
			CAST(1 AS DOUBLE),
			TIMESTAMP '2017-07-10 01:02:03.004 UTC',
			CAST('string' AS VARCHAR),
			ARRAY['A', 'B']
			)) AS t(col_tiny, col_small, col_int, col_big, col_real, col_double, col_ts, col_varchar, col_array )
			WHERE 1=1
			AND col_tiny = ?
			AND col_small = ?
			AND col_int = ?
			AND col_big = ?
			AND col_real = cast(? as real)
			AND col_double = cast(? as double)
			AND col_ts = ?
			AND col_varchar = ?
			AND col_array = ?`,
		int16(1),
		int16(1),
		int32(1),
		int64(1),
		Numeric("1"),
		Numeric("1"),
		time.Date(2017, 7, 10, 1, 2, 3, 4*1000000, time.UTC),
		"string",
		[]string{"A", "B"}).Scan(&value)
	if err != nil {
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
	if err := rows.Next([]driver.Value{result}); err != nil {
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

func TestIntegrationQueryContextCancellation(t *testing.T) {
	err := RegisterCustomClient("uncompressed", &http.Client{Transport: &http.Transport{DisableCompression: true}})
	if err != nil {
		t.Fatal(err)
	}
	dsn := *integrationServerFlag
	dsn += "?catalog=tpch&schema=sf100&source=cancel-test&custom_client=uncompressed"
	db := integrationOpen(t, dsn)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 3)
	done := make(chan struct{})
	longQuery := "SELECT COUNT(*) FROM lineitem"
	go func() {
		// query will complete in ~7s unless cancelled
		rows, err := db.QueryContext(ctx, longQuery)
		if err != nil {
			errCh <- err
			return
		}
		rows.Next()
		if err = rows.Err(); err != nil {
			errCh <- err
			return
		}
		close(done)
	}()

	// poll system.runtime.queries and wait for query to start working
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
			t.Fatal("failed to read query id", err)
		}
		if err = contextSleep(pollCtx, 100*time.Millisecond); err != nil {
			t.Fatal("query did not start in 1 second")
		}
	}

	cancel()

	select {
	case <-done:
		t.Fatal("unexpected query with cancelled context succeeded")
		break
	case err = <-errCh:
		if !strings.Contains(err.Error(), "canceled") {
			t.Fatal("expected err to be canceled but got:", err)
		}
	}

	// poll system.runtime.queries and wait for query to be cancelled
	pollCtx, pollCancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer pollCancel()
	for {
		row := db.QueryRowContext(pollCtx, "SELECT state, error_code FROM system.runtime.queries WHERE query_id = ?", queryID)
		var state string
		var code *string
		err := row.Scan(&state, &code)
		if err != nil {
			t.Fatal("failed to read query id", err)
		}
		if state == "FAILED" && code != nil && *code == "USER_CANCELED" {
			break
		}
		if err = contextSleep(pollCtx, 100*time.Millisecond); err != nil {
			t.Fatal("query was not cancelled in 1 second; state, code, err are:", state, code, err)
		}
	}
}

func TestIntegrationAccessToken(t *testing.T) {
	if tlsServer == "" {
		t.Skip("Skipping access token test when using a custom integration server.")
	}

	accessToken, err := generateToken()
	if err != nil {
		t.Fatal(err)
	}

	dsn := tlsServer + "?accessToken=" + accessToken

	db := integrationOpen(t, dsn)

	defer db.Close()
	rows, err := db.Query("SHOW CATALOGS")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count < 1 {
		t.Fatal("not enough rows returned:", count)
	}
}

func generateToken() (string, error) {
	privateKeyPEM, err := os.ReadFile("etc/secrets/private_key.pem")
	if err != nil {
		return "", fmt.Errorf("error reading private key file: %w", err)
	}

	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		return "", fmt.Errorf("error parsing private key: %w", err)
	}

	// Subject must be 'test'
	claims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * 365 * time.Hour)),
		Issuer:    "gotrino",
		Subject:   "test",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signedToken, err := token.SignedString(privateKey)

	if err != nil {
		return "", fmt.Errorf("error generating token: %w", err)
	}

	return signedToken, nil
}

func contextSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return ctx.Err()
	}
}

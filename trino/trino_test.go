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
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"runtime/debug"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	c := &Config{
		ServerURI:         "http://foobar@localhost:8080",
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "http://foobar@localhost:8080?session_properties=query_priority%3D1&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestConfigSSLCertPath(t *testing.T) {
	c := &Config{
		ServerURI:         "https://foobar@localhost:8080",
		SessionProperties: map[string]string{"query_priority": "1"},
		SSLCertPath:       "cert.pem",
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8080?SSLCertPath=cert.pem&session_properties=query_priority%3D1&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestConfigSSLCert(t *testing.T) {
	sslCert := `-----BEGIN CERTIFICATE-----
MIIFijCCA3ICCQDngXKCZFwSazANBgkqhkiG9w0BAQsFADCBhjELMAkGA1UEBhMC
WFgxEjAQBgNVBAgMCVN0YXRlTmFtZTERMA8GA1UEBwwIQ2l0eU5hbWUxFDASBgNV
BAoMC0NvbXBhbnlOYW1lMRswGQYDVQQLDBJDb21wYW55U2VjdGlvbk5hbWUxHTAb
BgNVBAMMFENvbW1vbk5hbWVPckhvc3RuYW1lMB4XDTIzMDUxNzE2MzQ0MloXDTMz
MDUxNDE2MzQ0MlowgYYxCzAJBgNVBAYTAlhYMRIwEAYDVQQIDAlTdGF0ZU5hbWUx
ETAPBgNVBAcMCENpdHlOYW1lMRQwEgYDVQQKDAtDb21wYW55TmFtZTEbMBkGA1UE
CwwSQ29tcGFueVNlY3Rpb25OYW1lMR0wGwYDVQQDDBRDb21tb25OYW1lT3JIb3N0
bmFtZTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAKzz/SIuOiHZbUAH
xCWrMaiJybdHHHl0smCu50XKvl/ZkszO1c4aES8/Vohw44ttaE+GOknTSGPka356
NqwdPYMjnXN0d5HY5T5nOfgLxGD/1iCHACrT4gkd1asJ7eFaUgud0a+e9+oG53Vh
Z3QV8+5JaWPuBMudJ8EOtrPMd0dJKVzeExTbpQLJ9HdIsHc6DXqshACd8Iy+ezqf
OoYMYyJMAHO86MZrTs3t9AwUADlvntrwwObVrZ3v43IOKwJTRnpImmVlkouKrGn/
HKzRmJEJ6hJQXhuhqI/0rr61XR8aa8Gs0FqtTTMJ32+PciPPzFtFVLAeA417lYz+
uXZ6IpTLK4oDH8Q6gJY80GYqcGc+01ZY90W2L+odTz9P74vnTvsUgSjOcy7prJ0+
WxoeBNPvkLeetX9WDZW4XaR++HVO1qelNJQqeB6Nver9MJdKkXvR3OxT6iluqXfA
l9JJ57tnzspSrttjWG4kwwiaGn/4xPqd95Hp0r1WAK8U0Cqtvz+Zw9jl341tC1Ya
K1KFIErZYf0KX8ZiYvmkHaTRxYiCmFnnfLtGdrAWkacisLKMhjeb9LXwC/TVtvio
a+ofiW2DX80pQptkfNJs9P19ZFEojPAEFHiZFpz5yZSxHglxIsdIhRsuy5xb/KTo
zey3tsKQJaFIah+aHKjyn3uZx2IRAgMBAAEwDQYJKoZIhvcNAQELBQADggIBAIs5
sbCMB6bT0hcNFqFRCI/BL23m5jwdL9kNWDlEQxBvErtzTC+uStGrCqwV+qu49QAZ
64kUolbzFyq/hQFpHd+9EzNkZGbiOf5toWaBUP6jaZzqYPdfDW+AwIA7iPHcqwH1
iWX2zuAWAICy4H+S4oa/ShOPc8BrrnS8k5f1NpergOhd+wl+szuXJN9Tjli3wd/k
L7f86xvZfOrEbss8YP4QE0+mKh6G71NLEVQ4SV7yIE2hCNLDFWS2ltGVRLv6CDaQ
fXIQrZx2Khvpj+HI/hrwm1wV8Cg5w2IvB831YjTSepSoos0Cc/qYC78zqol/NbwL
7TdHtuZKukDrisRiCDdoKFmS1/IUVeVR2352CG8G3Zo0wwfzoKLxLUtunnrKMmmO
r2jXykqP2hb1dApBNFM7FoaJ7a0j6EcURW8wYl4I+b9ymftPnnZ8mgrjwvLh5ETj
RgGsIBychLZoc1WWTZWu62+mvmSJnzEIFfaiSeYZLaL6qFHm6kqsAUn4s1Looj8/
XoCNjMecchWbpHGCPwMFH1k2smxu7bKk/RJNuWSVn1IPUceJnOBHZGj92aJGZpjr
8j39T3dK9F2r5rHwjZpeEIhyhbLw6pYKif+lBgAWJD3waG0ycwURA02/POHN4CpT
FKu5ZAlRfb2aYegr49DHhzoVAdInWQmP+5EZEUD1
-----END CERTIFICATE-----`
	c := &Config{
		ServerURI:         "https://foobar@localhost:8080",
		SessionProperties: map[string]string{"query_priority": "1"},
		SSLCert:           sslCert,
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8080?SSLCert=" + url.QueryEscape(sslCert) + "&session_properties=query_priority%3D1&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestExtraCredentials(t *testing.T) {
	c := &Config{
		ServerURI:        "http://foobar@localhost:8080",
		ExtraCredentials: map[string]string{"token": "mYtOkEn", "otherToken": "oThErToKeN"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "http://foobar@localhost:8080?extra_credentials=otherToken%3DoThErToKeN%2Ctoken%3DmYtOkEn&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestConfigWithoutSSLCertPath(t *testing.T) {
	c := &Config{
		ServerURI:         "https://foobar@localhost:8080",
		SessionProperties: map[string]string{"query_priority": "1"},
	}
	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8080?session_properties=query_priority%3D1&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestKerberosConfig(t *testing.T) {
	c := &Config{
		ServerURI:          "https://foobar@localhost:8090",
		SessionProperties:  map[string]string{"query_priority": "1"},
		KerberosEnabled:    "true",
		KerberosKeytabPath: "/opt/test.keytab",
		KerberosPrincipal:  "trino/testhost",
		KerberosRealm:      "example.com",
		KerberosConfigPath: "/etc/krb5.conf",
		SSLCertPath:        "/tmp/test.cert",
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8090?KerberosConfigPath=%2Fetc%2Fkrb5.conf&KerberosEnabled=true&KerberosKeytabPath=%2Fopt%2Ftest.keytab&KerberosPrincipal=trino%2Ftesthost&KerberosRealm=example.com&SSLCertPath=%2Ftmp%2Ftest.cert&session_properties=query_priority%3D1&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestInvalidKerberosConfig(t *testing.T) {
	c := &Config{
		ServerURI:       "http://foobar@localhost:8090",
		KerberosEnabled: "true",
	}

	_, err := c.FormatDSN()
	assert.Error(t, err, "dsn generated from invalid secure url, since kerberos enabled must has SSL enabled")
}

func TestConfigWithMalformedURL(t *testing.T) {
	_, err := (&Config{ServerURI: ":("}).FormatDSN()
	assert.Error(t, err, "dsn generated from malformed url")
}

func TestConnErrorDSN(t *testing.T) {
	testcases := []struct {
		Name string
		DSN  string
	}{
		{Name: "malformed", DSN: "://"},
		{Name: "unknown_client", DSN: "http://localhost?custom_client=unknown"},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			db, err := sql.Open("trino", tc.DSN)
			require.NoError(t, err)

			_, err = db.Query("SELECT 1")
			assert.Errorf(t, err, "test dsn is supposed to fail: %s", tc.DSN)

			if err == nil {
				require.NoError(t, db.Close())
			}
		})
	}
}

func TestRegisterCustomClientReserved(t *testing.T) {
	for _, tc := range []string{"true", "false"} {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			require.Errorf(t,
				RegisterCustomClient(tc, &http.Client{}),
				"client key name supposed to fail: %s", tc)
		})
	}
}

func TestRoundTripRetryQueryError(t *testing.T) {
	count := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count == 0 {
			count++
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&stmtResponse{
			Error: stmtError{
				ErrorName: "TEST",
			},
		})
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	assert.IsTypef(t, new(ErrQueryFailed), err, "unexpected error: %w", err)
}

func TestRoundTripCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)

	_, err = db.QueryContext(ctx, "SELECT 1")
	assert.Error(t, err, "unexpected query with cancelled context succeeded")
}

func TestAuthFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	assert.NoError(t, db.Close())
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
	statusMap map[time.Time]string
}

func (qpc *TestQueryProgressCallback) Update(qpi QueryProgressInfo) {
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

	statusMap := make(map[time.Time]string)
	progressUpdater := &TestQueryProgressCallback{
		statusMap: statusMap,
	}
	progressUpdaterPeriod, err := time.ParseDuration("1ms")

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
	}
}

func TestQueryColumns(t *testing.T) {
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

	rows, err := db.Query(`SELECT
  true AS bool,
  cast(123 AS tinyint) AS tinyint,
  cast(456 AS smallint) AS smallint,
  cast(678 AS integer) AS integer,
  cast(1234 AS bigint) AS bigint,
  cast(1.23 AS real) AS real,
  cast(1.23 AS double) AS double,
  cast(1.23 as decimal(10,5)) AS decimal,
  cast('aaa' as varchar) AS vunbounded,
  cast('bbb' as varchar(10)) AS vbounded,
  cast('ccc' AS char) AS cunbounded,
  cast('ddd' as char(10)) AS cbounded,
  cast('ddd' as varbinary) AS varbinary,
  cast('{"aaa": 1}' as json) AS json,
  current_date AS date,
  cast(current_time as time) AS time,
  cast(current_time as time(6)) AS timep,
  cast(current_time as time with time zone) AS timetz,
  cast(current_time as timestamp) AS ts,
  cast(current_time as timestamp(6)) AS tsp,
  cast(current_time as timestamp with time zone) AS tstz,
  cast(current_time as timestamp(6) with time zone) AS tsptz,
  interval '3' month AS ytm,
  interval '2' day AS dts,
  array['a', 'b'] AS varray,
  array[array['a'], array['b']] AS v2array,
  array[array[array['a'], array['b']]] AS v3array,
  map(array['a'], array[1]) AS map,
  array[map(array['a'], array[1]), map(array['b'], array[2])] AS marray,
  row('a', 1) AS row,
  cast(row('a', 1.23) AS row(x varchar, y double)) AS named_row,
  ipaddress '10.0.0.1' AS ip,
  uuid '12151fd2-7586-11e9-8f9e-2a86e4085a59' AS uuid`)
	require.NoError(t, err, "Failed executing query")
	assert.NotNil(t, rows)

	columns, err := rows.Columns()
	require.NoError(t, err, "Failed reading result columns")

	assert.Equal(t, 33, len(columns), "Expected 33 result column")
	expectedNames := []string{
		"bool",
		"tinyint",
		"smallint",
		"integer",
		"bigint",
		"real",
		"double",
		"decimal",
		"vunbounded",
		"vbounded",
		"cunbounded",
		"cbounded",
		"varbinary",
		"json",
		"date",
		"time",
		"timep",
		"timetz",
		"ts",
		"tsp",
		"tstz",
		"tsptz",
		"ytm",
		"dts",
		"varray",
		"v2array",
		"v3array",
		"map",
		"marray",
		"row",
		"named_row",
		"ip",
		"uuid",
	}
	assert.Equal(t, expectedNames, columns)

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err, "Failed reading result column types")

	assert.Equal(t, 33, len(columnTypes), "Expected 33 result column type")

	type columnType struct {
		typeName  string
		hasScale  bool
		precision int64
		scale     int64
		hasLength bool
		length    int64
		scanType  reflect.Type
	}
	expectedTypes := []columnType{
		{
			"BOOLEAN",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullBool{}),
		},
		{
			"TINYINT",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullInt32{}),
		},
		{
			"SMALLINT",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullInt32{}),
		},
		{
			"INTEGER",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullInt32{}),
		},
		{
			"BIGINT",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullInt64{}),
		},
		{
			"REAL",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullFloat64{}),
		},
		{
			"DOUBLE",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullFloat64{}),
		},
		{
			"DECIMAL",
			true,
			10,
			5,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"VARCHAR",
			false,
			0,
			0,
			true,
			math.MaxInt32,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"VARCHAR",
			false,
			0,
			0,
			true,
			10,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"CHAR",
			false,
			0,
			0,
			true,
			1,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"CHAR",
			false,
			0,
			0,
			true,
			10,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"VARBINARY",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"JSON",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"DATE",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIME",
			true,
			3,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIME",
			true,
			6,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIME WITH TIME ZONE",
			true,
			3,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP",
			true,
			3,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP",
			true,
			6,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP WITH TIME ZONE",
			true,
			3,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP WITH TIME ZONE",
			true,
			6,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"INTERVAL YEAR TO MONTH",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"INTERVAL DAY TO SECOND",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"ARRAY(VARCHAR(1))",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(NullSliceString{}),
		},
		{
			"ARRAY(ARRAY(VARCHAR(1)))",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(NullSlice2String{}),
		},
		{
			"ARRAY(ARRAY(ARRAY(VARCHAR(1))))",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(NullSlice3String{}),
		},
		{
			"MAP(VARCHAR(1), INTEGER)",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(NullMap{}),
		},
		{
			"ARRAY(MAP(VARCHAR(1), INTEGER))",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(NullSliceMap{}),
		},
		{
			"ROW(VARCHAR(1), INTEGER)",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(new(interface{})).Elem(),
		},
		{
			"ROW(X VARCHAR, Y DOUBLE)",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(new(interface{})).Elem(),
		},
		{
			"IPADDRESS",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"UUID",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
	}
	actualTypes := make([]columnType, 33)
	for i, column := range columnTypes {
		actualTypes[i].typeName = column.DatabaseTypeName()
		actualTypes[i].precision, actualTypes[i].scale, actualTypes[i].hasScale = column.DecimalSize()
		actualTypes[i].length, actualTypes[i].hasLength = column.Length()
		actualTypes[i].scanType = column.ScanType()
	}

	assert.Equal(t, actualTypes, expectedTypes)
}

func TestMaxGoPrecisionDateTime(t *testing.T) {
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

	rows, err := db.Query(`SELECT
  cast(current_time as time(9)) AS timep,
  cast(current_time as time(9) with time zone) AS timeptz,
  cast(current_time as timestamp(9)) AS tsp,
  cast(current_time as timestamp(9) with time zone) AS tsptz`)
	require.NoError(t, err, "Failed executing query")
	assert.NotNil(t, rows)

	columns, err := rows.Columns()
	require.NoError(t, err, "Failed reading result columns")

	assert.Equal(t, 4, len(columns), "Expected 4 result column")
	expectedNames := []string{
		"timep",
		"timeptz",
		"tsp",
		"tsptz",
	}
	assert.Equal(t, expectedNames, columns)

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err, "Failed reading result column types")

	assert.Equal(t, 4, len(columnTypes), "Expected 4 result column type")

	type columnType struct {
		typeName  string
		hasScale  bool
		precision int64
		scale     int64
		hasLength bool
		length    int64
		scanType  reflect.Type
	}
	expectedTypes := []columnType{
		{
			"TIME",
			true,
			9,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIME WITH TIME ZONE",
			true,
			9,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP",
			true,
			9,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP WITH TIME ZONE",
			true,
			9,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
	}
	actualTypes := make([]columnType, 4)
	for i, column := range columnTypes {
		actualTypes[i].typeName = column.DatabaseTypeName()
		actualTypes[i].precision, actualTypes[i].scale, actualTypes[i].hasScale = column.DecimalSize()
		actualTypes[i].length, actualTypes[i].hasLength = column.Length()
		actualTypes[i].scanType = column.ScanType()
	}

	assert.Equal(t, actualTypes, expectedTypes)

	assert.True(t, rows.Next())
	require.NoError(t, rows.Err())

}

func TestQueryCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&stmtResponse{
			Error: stmtError{
				ErrorName: "USER_CANCELLED",
			},
		})
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	assert.EqualError(t, err, ErrQueryCancelled.Error(), "unexpected error")
}

func TestQueryFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	assert.IsTypef(t, new(ErrQueryFailed), err, "unexpected error: %w", err)
}

// This test ensures that the fetch method is not generating stack overflow errors.
// === RUN   TestFetchNoStackOverflow
// runtime: goroutine stack exceeds 1000000000-byte limit
// runtime: sp=0x14037b00390 stack=[0x14037b00000, 0x14057b00000]
// fatal error: stack overflow
func TestFetchNoStackOverflow(t *testing.T) {
	previousSetting := debug.SetMaxStack(50 * 1024)
	defer debug.SetMaxStack(previousSetting)
	count := 0
	var buf *bytes.Buffer
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count <= 50 {
			if buf == nil {
				buf = new(bytes.Buffer)
				json.NewEncoder(buf).Encode(&stmtResponse{
					ID:      "fake-query",
					NextURI: ts.URL + "/v1/statement/20210817_140827_00000_arvdv/1",
				})
			}
			w.WriteHeader(http.StatusOK)
			w.Write(buf.Bytes())
			count++
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&stmtResponse{
			Error: stmtError{
				ErrorName: "TEST",
			},
		})
	}))

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	assert.IsTypef(t, new(ErrQueryFailed), err, "unexpected error: %w", err)

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

func TestUnsupportedHeader(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(trinoSetRoleHeader, "foo")
		w.WriteHeader(http.StatusOK)
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	assert.EqualError(t, err, ErrUnsupportedHeader.Error(), "unexpected error")
}

func TestSSLCertPath(t *testing.T) {
	db, err := sql.Open("trino", "https://localhost:9?SSLCertPath=/tmp/invalid_test.cert")
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	want := "Error loading SSL Cert File"
	err = db.Ping()
	require.Error(t, err)
	require.Contains(t, err.Error(), want)
}

func TestWithoutSSLCertPath(t *testing.T) {
	db, err := sql.Open("trino", "https://localhost:9")
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	assert.NoError(t, db.Ping())
}

func TestUnsupportedTransaction(t *testing.T) {
	db, err := sql.Open("trino", "http://localhost:9")
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Begin()
	require.Error(t, err, "unsupported transaction succeeded with no error")

	expected := "operation not supported"
	assert.Contains(t, err.Error(), expected)
}

func TestTypeConversion(t *testing.T) {
	utc, err := time.LoadLocation("UTC")
	require.NoError(t, err)
	paris, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)

	testcases := []struct {
		DataType                   string
		RawType                    string
		Arguments                  []typeArgument
		ResponseUnmarshalledSample interface{}
		ExpectedGoValue            interface{}
	}{
		{
			DataType:                   "boolean",
			RawType:                    "boolean",
			ResponseUnmarshalledSample: true,
			ExpectedGoValue:            true,
		},
		{
			DataType:                   "varchar(1)",
			RawType:                    "varchar",
			ResponseUnmarshalledSample: "hello",
			ExpectedGoValue:            "hello",
		},
		{
			DataType:                   "bigint",
			RawType:                    "bigint",
			ResponseUnmarshalledSample: json.Number("1234516165077230279"),
			ExpectedGoValue:            int64(1234516165077230279),
		},
		{
			DataType:                   "double",
			RawType:                    "double",
			ResponseUnmarshalledSample: json.Number("1.0"),
			ExpectedGoValue:            float64(1),
		},
		{
			DataType:                   "date",
			RawType:                    "date",
			ResponseUnmarshalledSample: "2017-07-10",
			ExpectedGoValue:            time.Date(2017, 7, 10, 0, 0, 0, 0, time.Local),
		},
		{
			DataType:                   "time",
			RawType:                    "time",
			ResponseUnmarshalledSample: "01:02:03.000",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.Local),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000 UTC",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, utc),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000 +03:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000+03:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000 -05:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", -5*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000-05:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", -5*3600)),
		},
		{
			DataType:                   "time",
			RawType:                    "time",
			ResponseUnmarshalledSample: "01:02:03.123456789",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, time.Local),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789 UTC",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, utc),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789 +03:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789+03:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789 -05:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", -5*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789-05:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", -5*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789 Europe/Paris",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, paris),
		},
		{
			DataType:                   "timestamp",
			RawType:                    "timestamp",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.Local),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000 UTC",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, utc),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000 +03:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000+03:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000 -04:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", -4*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000-04:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", -4*3600)),
		},
		{
			DataType:                   "timestamp",
			RawType:                    "timestamp",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.Local),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789 UTC",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, utc),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789 +03:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789+03:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789 -04:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", -4*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789-04:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", -4*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789 Europe/Paris",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, paris),
		},
		{
			DataType: "map(varchar,varchar)",
			RawType:  "map",
			Arguments: []typeArgument{
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "varchar",
						},
					},
				},
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "varchar",
						},
					},
				},
			},
			ResponseUnmarshalledSample: nil,
			ExpectedGoValue:            nil,
		},
		{
			// arrays return data as-is for slice scanners
			DataType: "array(varchar)",
			RawType:  "array",
			Arguments: []typeArgument{
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "varchar",
						},
					},
				},
			},
			ResponseUnmarshalledSample: nil,
			ExpectedGoValue:            nil,
		},
		{
			// rows return data as-is for slice scanners
			DataType: "row(int, varchar(1), timestamp, array(varchar(1)))",
			RawType:  "row",
			Arguments: []typeArgument{
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "integer",
						},
					},
				},
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "varchar",
							Arguments: []typeArgument{
								{
									Kind: "LONG",
									long: 1,
								},
							},
						},
					},
				},
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "timestamp",
						},
					},
				},
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "array",
							Arguments: []typeArgument{
								{
									Kind: "TYPE",
									typeSignature: typeSignature{
										RawType: "varchar",
										Arguments: []typeArgument{
											{
												Kind: "LONG",
												long: 1,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			ResponseUnmarshalledSample: []interface{}{
				json.Number("1"),
				"a",
				"2017-07-10 01:02:03.000 UTC",
				[]interface{}{"b"},
			},
			ExpectedGoValue: []interface{}{
				json.Number("1"),
				"a",
				"2017-07-10 01:02:03.000 UTC",
				[]interface{}{"b"},
			},
		},
	}

	for _, tc := range testcases {
		converter, err := newTypeConverter(tc.DataType, typeSignature{RawType: tc.RawType, Arguments: tc.Arguments})
		assert.NoError(t, err)

		t.Run(tc.DataType+":nil", func(t *testing.T) {
			_, err := converter.ConvertValue(nil)
			assert.NoError(t, err)
		})

		t.Run(tc.DataType+":bogus", func(t *testing.T) {
			_, err := converter.ConvertValue(struct{}{})
			assert.Error(t, err, "bogus data scanned with no error")
		})

		t.Run(tc.DataType+":sample", func(t *testing.T) {
			v, err := converter.ConvertValue(tc.ResponseUnmarshalledSample)
			require.NoError(t, err)

			require.Equal(t,
				v, tc.ExpectedGoValue,
				"unexpected data from sample:\nhave %+v\nwant %+v", v, tc.ExpectedGoValue)
		})
	}
}

func TestSliceTypeConversion(t *testing.T) {
	testcases := []struct {
		GoType                          string
		Scanner                         sql.Scanner
		TrinoResponseUnmarshalledSample interface{}
		TestScanner                     func(t *testing.T, s sql.Scanner, isValid bool)
	}{
		{
			GoType:                          "[]bool",
			Scanner:                         &NullSliceBool{},
			TrinoResponseUnmarshalledSample: []interface{}{true},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceBool)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[]string",
			Scanner:                         &NullSliceString{},
			TrinoResponseUnmarshalledSample: []interface{}{"hello"},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceString)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[]int64",
			Scanner:                         &NullSliceInt64{},
			TrinoResponseUnmarshalledSample: []interface{}{json.Number("1")},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceInt64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},

		{
			GoType:                          "[]float64",
			Scanner:                         &NullSliceFloat64{},
			TrinoResponseUnmarshalledSample: []interface{}{json.Number("1.0")},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceFloat64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[]time.Time",
			Scanner:                         &NullSliceTime{},
			TrinoResponseUnmarshalledSample: []interface{}{"2017-07-01"},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceTime)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[]map[string]interface{}",
			Scanner:                         &NullSliceMap{},
			TrinoResponseUnmarshalledSample: []interface{}{map[string]interface{}{"hello": "world"}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceMap)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.GoType+":nil", func(t *testing.T) {
			assert.NoError(t, tc.Scanner.Scan(nil))
		})

		t.Run(tc.GoType+":bogus", func(t *testing.T) {
			assert.Error(t, tc.Scanner.Scan(struct{}{}))
			assert.Error(t, tc.Scanner.Scan([]interface{}{struct{}{}}), "bogus data scanned with no error")
		})

		t.Run(tc.GoType+":sample", func(t *testing.T) {
			require.NoError(t, tc.Scanner.Scan(tc.TrinoResponseUnmarshalledSample))
			tc.TestScanner(t, tc.Scanner, true)
			require.NoError(t, tc.Scanner.Scan(nil))
			tc.TestScanner(t, tc.Scanner, false)
		})
	}
}

func TestSlice2TypeConversion(t *testing.T) {
	testcases := []struct {
		GoType                          string
		Scanner                         sql.Scanner
		TrinoResponseUnmarshalledSample interface{}
		TestScanner                     func(t *testing.T, s sql.Scanner, isValid bool)
	}{
		{
			GoType:                          "[][]bool",
			Scanner:                         &NullSlice2Bool{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{true}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2Bool)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][]string",
			Scanner:                         &NullSlice2String{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{"hello"}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2String)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][]int64",
			Scanner:                         &NullSlice2Int64{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{json.Number("1")}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2Int64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][]float64",
			Scanner:                         &NullSlice2Float64{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{json.Number("1.0")}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2Float64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][]time.Time",
			Scanner:                         &NullSlice2Time{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{"2017-07-01"}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2Time)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][]map[string]interface{}",
			Scanner:                         &NullSlice2Map{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{map[string]interface{}{"hello": "world"}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2Map)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.GoType+":nil", func(t *testing.T) {
			assert.NoError(t, tc.Scanner.Scan(nil))
			assert.NoError(t, tc.Scanner.Scan([]interface{}{nil}))
		})

		t.Run(tc.GoType+":bogus", func(t *testing.T) {
			assert.Error(t, tc.Scanner.Scan(struct{}{}), "bogus data scanned with no error")
			assert.Error(t, tc.Scanner.Scan([]interface{}{struct{}{}}), "bogus data scanned with no error")
			assert.Error(t, tc.Scanner.Scan([]interface{}{[]interface{}{struct{}{}}}), "bogus data scanned with no error")
		})

		t.Run(tc.GoType+":sample", func(t *testing.T) {
			require.NoError(t, tc.Scanner.Scan(tc.TrinoResponseUnmarshalledSample))
			tc.TestScanner(t, tc.Scanner, true)
			require.NoError(t, tc.Scanner.Scan(nil))
			tc.TestScanner(t, tc.Scanner, false)
		})
	}
}

func TestSlice3TypeConversion(t *testing.T) {
	testcases := []struct {
		GoType                          string
		Scanner                         sql.Scanner
		TrinoResponseUnmarshalledSample interface{}
		TestScanner                     func(t *testing.T, s sql.Scanner, isValid bool)
	}{
		{
			GoType:                          "[][][]bool",
			Scanner:                         &NullSlice3Bool{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{true}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3Bool)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][][]string",
			Scanner:                         &NullSlice3String{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{"hello"}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3String)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][][]int64",
			Scanner:                         &NullSlice3Int64{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{json.Number("1")}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3Int64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][][]float64",
			Scanner:                         &NullSlice3Float64{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{json.Number("1.0")}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3Float64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][][]time.Time",
			Scanner:                         &NullSlice3Time{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{"2017-07-01"}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3Time)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][][]map[string]interface{}",
			Scanner:                         &NullSlice3Map{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{map[string]interface{}{"hello": "world"}}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3Map)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.GoType+":nil", func(t *testing.T) {
			assert.NoError(t, tc.Scanner.Scan(nil))
			assert.NoError(t, tc.Scanner.Scan([]interface{}{[]interface{}{nil}}))
		})

		t.Run(tc.GoType+":bogus", func(t *testing.T) {
			assert.Error(t, tc.Scanner.Scan(struct{}{}), "bogus data scanned with no error")
			assert.Error(t, tc.Scanner.Scan([]interface{}{[]interface{}{struct{}{}}}), "bogus data scanned with no error")
			assert.Error(t, tc.Scanner.Scan([]interface{}{[]interface{}{[]interface{}{struct{}{}}}}), "bogus data scanned with no error")
		})

		t.Run(tc.GoType+":sample", func(t *testing.T) {
			require.NoError(t, tc.Scanner.Scan(tc.TrinoResponseUnmarshalledSample))
			tc.TestScanner(t, tc.Scanner, true)
			require.NoError(t, tc.Scanner.Scan(nil))
			tc.TestScanner(t, tc.Scanner, false)
		})
	}
}

func BenchmarkQuery(b *testing.B) {
	c := &Config{
		ServerURI:         *integrationServerFlag,
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(b, err)

	db, err := sql.Open("trino", dsn)
	require.NoError(b, err)

	b.Cleanup(func() {
		assert.NoError(b, db.Close())
	})

	q := `SELECT * FROM tpch.sf1.orders LIMIT 10000000`
	for n := 0; n < b.N; n++ {
		rows, err := db.Query(q)
		require.NoError(b, err)
		for rows.Next() {
		}
		rows.Close()
	}
}

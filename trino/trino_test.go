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
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

	if rows != nil {
		for rows.Next() {
			var ts string
			require.NoError(t, rows.Scan(&ts), "Failed scanning query result")

			want := "TestUser"
			assert.Equal(t, want, ts, "Expected value does not equal result value")
		}
	}
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

	testcases := []struct {
		DataType                   string
		ResponseUnmarshalledSample interface{}
		ExpectedGoValue            interface{}
	}{
		{
			DataType:                   "boolean",
			ResponseUnmarshalledSample: true,
			ExpectedGoValue:            true,
		},
		{
			DataType:                   "varchar(1)",
			ResponseUnmarshalledSample: "hello",
			ExpectedGoValue:            "hello",
		},
		{
			DataType:                   "bigint",
			ResponseUnmarshalledSample: json.Number("1234516165077230279"),
			ExpectedGoValue:            int64(1234516165077230279),
		},
		{
			DataType:                   "double",
			ResponseUnmarshalledSample: json.Number("1.0"),
			ExpectedGoValue:            float64(1),
		},
		{
			DataType:                   "date",
			ResponseUnmarshalledSample: "2017-07-10",
			ExpectedGoValue:            time.Date(2017, 7, 10, 0, 0, 0, 0, time.Local),
		},
		{
			DataType:                   "time",
			ResponseUnmarshalledSample: "01:02:03.000",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.Local),
		},
		{
			DataType:                   "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000 UTC",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, utc),
		},
		{
			DataType:                   "timestamp",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.Local),
		},
		{
			DataType:                   "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000 UTC",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, utc),
		},
		{
			DataType:                   "map",
			ResponseUnmarshalledSample: nil,
			ExpectedGoValue:            nil,
		},
		{
			// arrays return data as-is for slice scanners
			DataType:                   "array",
			ResponseUnmarshalledSample: nil,
			ExpectedGoValue:            nil,
		},
		{
			// rows return data as-is for slice scanners
			DataType: "row(int, varchar(1), timestamp, array(varchar(1)))",
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
		converter := newTypeConverter(tc.DataType)

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

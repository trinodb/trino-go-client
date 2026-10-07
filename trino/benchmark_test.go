package trino

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const benchmarkPageRows = 2000

// benchmarkPageColumns covers the shapes a page carries: numbers, strings,
// booleans, nulls, arrays and maps.
var benchmarkPageColumns = []struct {
	name, dataType, signature string
	value                     func(row int) string
}{
	{"bigint", "bigint", `{"rawType":"bigint","arguments":[]}`, func(row int) string { return fmt.Sprint(row * 1000003) }},
	{"integer", "integer", `{"rawType":"integer","arguments":[]}`, func(row int) string { return fmt.Sprint(row) }},
	{"smallint", "smallint", `{"rawType":"smallint","arguments":[]}`, func(row int) string { return fmt.Sprint(row % 30000) }},
	{"tinyint", "tinyint", `{"rawType":"tinyint","arguments":[]}`, func(row int) string { return fmt.Sprint(row % 100) }},
	{"double", "double", `{"rawType":"double","arguments":[]}`, func(row int) string { return fmt.Sprint(float64(row)*1.5 + 0.25) }},
	{"real", "real", `{"rawType":"real","arguments":[]}`, func(row int) string { return fmt.Sprint(float32(row) / 4) }},
	{"decimal", "decimal(10,2)", `{"rawType":"decimal","arguments":[{"kind":"LONG","value":10},{"kind":"LONG","value":2}]}`, func(row int) string { return fmt.Sprintf(`"%d.%02d"`, row, row%100) }},
	{"boolean", "boolean", `{"rawType":"boolean","arguments":[]}`, func(row int) string { return fmt.Sprint(row%2 == 0) }},
	{"varchar", "varchar", `{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}`, func(row int) string { return fmt.Sprintf(`"name-%d"`, row) }},
	{"date", "date", `{"rawType":"date","arguments":[]}`, func(int) string { return `"2026-10-07"` }},
	{"time", "time(3)", `{"rawType":"time","arguments":[{"kind":"LONG","value":3}]}`, func(int) string { return `"12:34:56.789"` }},
	{"timestamp", "timestamp(3)", `{"rawType":"timestamp","arguments":[{"kind":"LONG","value":3}]}`, func(int) string { return `"2026-10-07 12:34:56.789"` }},
	{"varbinary", "varbinary", `{"rawType":"varbinary","arguments":[]}`, func(int) string { return `"AQIDBA=="` }},
	{"array", "array(bigint)", `{"rawType":"array","arguments":[{"kind":"TYPE","value":{"rawType":"bigint","arguments":[]}}]}`, func(row int) string { return fmt.Sprintf("[%d,%d,%d]", row, row+1, row+2) }},
	{"map", "map(varchar,double)", `{"rawType":"map","arguments":[{"kind":"TYPE","value":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}},{"kind":"TYPE","value":{"rawType":"double","arguments":[]}}]}`, func(row int) string { return fmt.Sprintf(`{"a":%d.5,"b":2.5}`, row) }},
	{"nullable", "varchar", `{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}`, func(row int) string {
		if row%3 == 0 {
			return "null"
		}
		return `"present"`
	}},
}

// BenchmarkPageDecoding measures decoding a single large page of the direct
// protocol, served from memory so that the JSON decoding dominates.
func BenchmarkPageDecoding(b *testing.B) {
	page := benchmarkPage()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			fmt.Fprintf(w, `{"id":%q,"nextUri":%q,"stats":{"state":"QUEUED"}}`, fakeQueryID, server.URL+"/v1/statement/"+fakeQueryID+"/1")
			return
		}
		_, _ = w.Write(page)
	}))
	b.Cleanup(server.Close)
	db, err := sql.Open("trino", server.URL)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, db.Close()) })

	b.SetBytes(int64(len(page)))
	b.ReportAllocs()
	for b.Loop() {
		rows, err := db.Query("SELECT 1")
		require.NoError(b, err)
		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(b, rows.Err())
		require.NoError(b, rows.Close())
		require.Equal(b, benchmarkPageRows, count)
	}
}

func benchmarkPage() []byte {
	var page strings.Builder
	fmt.Fprintf(&page, `{"id":%q,"stats":{"state":"FINISHED"},"columns":[`, fakeQueryID)
	for i, column := range benchmarkPageColumns {
		if i > 0 {
			page.WriteByte(',')
		}
		fmt.Fprintf(&page, `{"name":%q,"type":%q,"typeSignature":%s}`, column.name, column.dataType, column.signature)
	}
	page.WriteString(`],"data":[`)
	for row := range benchmarkPageRows {
		if row > 0 {
			page.WriteByte(',')
		}
		page.WriteByte('[')
		for i, column := range benchmarkPageColumns {
			if i > 0 {
				page.WriteByte(',')
			}
			page.WriteString(column.value(row))
		}
		page.WriteByte(']')
	}
	page.WriteString(`]}`)
	return []byte(page.String())
}

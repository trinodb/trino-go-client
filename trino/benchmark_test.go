package trino

import (
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	benchmarkPageRows    = 2000
	benchmarkSegmentRows = 20000
)

type benchmarkColumn struct {
	name, dataType, signature string
	value                     func(row int) string
}

// benchmarkPageColumns covers the shapes a page carries: numbers, strings,
// booleans, nulls, arrays and maps.
var benchmarkPageColumns = []benchmarkColumn{
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

// benchmarkSegmentColumns follow the TPC-H orders table, a typical shape
// of the large results the spooling protocol is used for.
var benchmarkSegmentColumns = []benchmarkColumn{
	{"orderkey", "bigint", `{"rawType":"bigint","arguments":[]}`, func(row int) string { return fmt.Sprint(row*4 + 1) }},
	{"custkey", "bigint", `{"rawType":"bigint","arguments":[]}`, func(row int) string { return fmt.Sprint(row%150000 + 1) }},
	{"orderstatus", "varchar(1)", `{"rawType":"varchar","arguments":[{"kind":"LONG","value":1}]}`, func(row int) string { return `"` + string("OFP"[row%3]) + `"` }},
	{"totalprice", "double", `{"rawType":"double","arguments":[]}`, func(row int) string { return fmt.Sprint(float64(row%500000)*1.37 + 857.71) }},
	{"orderdate", "date", `{"rawType":"date","arguments":[]}`, func(row int) string {
		return time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, row%2400).Format(`"2006-01-02"`)
	}},
	{"orderpriority", "varchar(15)", `{"rawType":"varchar","arguments":[{"kind":"LONG","value":15}]}`, func(row int) string { return fmt.Sprintf(`"%d-PRIORITY"`, row%5+1) }},
	{"clerk", "varchar(15)", `{"rawType":"varchar","arguments":[{"kind":"LONG","value":15}]}`, func(row int) string { return fmt.Sprintf(`"Clerk#%09d"`, row%1000+1) }},
	{"shippriority", "integer", `{"rawType":"integer","arguments":[]}`, func(int) string { return "0" }},
	{"comment", "varchar(79)", `{"rawType":"varchar","arguments":[{"kind":"LONG","value":79}]}`, func(row int) string {
		return fmt.Sprintf(`"nstructions sleep furiously among %d final requests. carefully"`, row)
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

// BenchmarkSegmentDecoding measures decoding an uncompressed spooled
// segment, the work each decoding worker does.
func BenchmarkSegmentDecoding(b *testing.B) {
	segment := []byte(benchmarkRows(benchmarkSegmentColumns, benchmarkSegmentRows))
	columns := make([]queryColumn, len(benchmarkSegmentColumns))
	for i, column := range benchmarkSegmentColumns {
		columns[i] = queryColumn{Name: column.name, Type: column.dataType}
		require.NoError(b, json.Unmarshal([]byte(column.signature), &columns[i].TypeSignature))
		require.NoError(b, unmarshalArguments(&columns[i].TypeSignature))
	}
	decoder, err := newRowsDecoder(columns, time.UTC)
	require.NoError(b, err)
	metadata := segmentMetadata{segmentSize: int64(len(segment)), rowsCount: newOptionalInt64(benchmarkSegmentRows)}

	b.SetBytes(int64(len(segment)))
	b.ReportAllocs()
	for b.Loop() {
		rows, err := decodeSegment(segment, "json", metadata, decoder)
		require.NoError(b, err)
		require.Len(b, rows, benchmarkSegmentRows)
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
	page.WriteString(`],"data":`)
	page.WriteString(benchmarkRows(benchmarkPageColumns, benchmarkPageRows))
	page.WriteString(`}`)
	return []byte(page.String())
}

// benchmarkRows returns the JSON array of rows rows of columns.
func benchmarkRows(columns []benchmarkColumn, rows int) string {
	var data strings.Builder
	data.WriteByte('[')
	for row := range rows {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteByte('[')
		for i, column := range columns {
			if i > 0 {
				data.WriteByte(',')
			}
			data.WriteString(column.value(row))
		}
		data.WriteByte(']')
	}
	data.WriteByte(']')
	return data.String()
}

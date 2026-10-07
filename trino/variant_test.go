package trino

import (
	"database/sql"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// variantVector is a VARIANT value as Trino 483 sent it for expression, once
// with the VARIANT_BINARY capability, as metadata and value, and once with
// only VARIANT, as JSON text. testdata/variant_vectors.json holds vectors
// for every primitive type, and for arrays and objects with 1, 2 and 4 byte
// element counts, offsets, field IDs and metadata offsets.
type variantVector struct {
	Expression string `json:"expression"`
	Metadata   string `json:"metadata"`
	Value      string `json:"value"`
	JSON       string `json:"json"`
}

func TestVariantStringMatchesTheServerJSON(t *testing.T) {
	t.Parallel()
	content, err := os.ReadFile("testdata/variant_vectors.json")
	require.NoError(t, err)
	var vectors []variantVector
	require.NoError(t, json.Unmarshal(content, &vectors))
	require.NotEmpty(t, vectors)

	for _, vector := range vectors {
		t.Run(vector.Expression, func(t *testing.T) {
			t.Parallel()
			variant := decodeVariantVector(t, vector.Metadata, vector.Value, time.UTC)

			assert.Equal(t, vector.JSON, variant.String())
			marshaled, err := json.Marshal(variant)
			require.NoError(t, err)
			assert.JSONEq(t, vector.JSON, string(marshaled))
		})
	}
}

func TestVariantValue(t *testing.T) {
	t.Parallel()
	location := time.FixedZone("UTC+2", 2*60*60)
	cases := []struct {
		expression string
		metadata   string
		value      string
		wantType   VariantType
		want       interface{}
	}{
		{expression: "CAST(JSON 'null' AS VARIANT)", value: "AA==", wantType: VariantNull, want: nil},
		{expression: "CAST(true AS VARIANT)", value: "BA==", wantType: VariantBoolean, want: true},
		{expression: "CAST(false AS VARIANT)", value: "CA==", wantType: VariantBoolean, want: false},
		{expression: "CAST(TINYINT '-7' AS VARIANT)", value: "DPk=", wantType: VariantInt8, want: int64(-7)},
		{expression: "CAST(SMALLINT '-300' AS VARIANT)", value: "ENT+", wantType: VariantInt16, want: int64(-300)},
		{expression: "CAST(70000 AS VARIANT)", value: "FHARAQA=", wantType: VariantInt32, want: int64(70000)},
		{expression: "CAST(BIGINT '-5000000000' AS VARIANT)", value: "GAAO+tX+////", wantType: VariantInt64, want: int64(-5000000000)},
		{expression: "CAST(REAL '0.1' AS VARIANT)", value: "OM3MzD0=", wantType: VariantFloat, want: float32(0.1)},
		{expression: "CAST(DOUBLE '1e10' AS VARIANT)", value: "HAAAACBfoAJC", wantType: VariantDouble, want: 1e10},
		{expression: "CAST(DECIMAL '-12.345' AS VARIANT)", value: "IAPHz///", wantType: VariantDecimal, want: Numeric("-12.345")},
		{expression: "CAST(DECIMAL '1234567890.12345678' AS VARIANT)", value: "JAhO8zCmS5u2AQ==", wantType: VariantDecimal, want: Numeric("1234567890.12345678")},
		{
			expression: "CAST(DECIMAL '-12345678901234567890.123456789012345678' AS VARIANT)",
			value:      "KBKyDMchr2+2O+zM/Q8JT7b2",
			wantType:   VariantDecimal,
			want:       Numeric("-12345678901234567890.123456789012345678"),
		},
		{expression: "CAST(DECIMAL '0.0000001' AS VARIANT)", value: "IAcBAAAA", wantType: VariantDecimal, want: Numeric("0.0000001")},
		{expression: "CAST(DATE '1969-12-31' AS VARIANT)", value: "LP////8=", wantType: VariantDate, want: time.Date(1969, 12, 31, 0, 0, 0, 0, location)},
		{expression: "CAST(TIME '03:04:05.123456' AS VARIANT)", value: "RIA1V5ICAAAA", wantType: VariantTimeNTZMicros, want: time.Date(0, 1, 1, 3, 4, 5, 123456000, location)},
		{
			expression: "CAST(TIMESTAMP '2020-01-02 03:04:05.123456' AS VARIANT)",
			value:      "NIDVKHIfmwUA",
			wantType:   VariantTimestampNTZMicros,
			want:       time.Date(2020, 1, 2, 3, 4, 5, 123456000, location),
		},
		{
			expression: "CAST(TIMESTAMP '1969-12-31 23:59:59.999999999' AS VARIANT)",
			value:      "TP//////////",
			wantType:   VariantTimestampNTZNanos,
			want:       time.Date(1969, 12, 31, 23, 59, 59, 999999999, location),
		},
		{
			expression: "CAST(TIMESTAMP '2020-01-02 03:04:05.123456 America/New_York' AS VARIANT)",
			value:      "MIAJC6MjmwUA",
			wantType:   VariantTimestampUTCMicros,
			want:       time.Date(2020, 1, 2, 8, 4, 5, 123456000, time.UTC),
		},
		{
			expression: "CAST(TIMESTAMP '2020-01-02 03:04:05.123456789 +02:00' AS VARIANT)",
			value:      "SBW/EI5J7OUV",
			wantType:   VariantTimestampUTCNanos,
			want:       time.Date(2020, 1, 2, 1, 4, 5, 123456789, time.UTC),
		},
		{expression: "CAST(X'00ff10' AS VARIANT)", value: "PAMAAAAA/xA=", wantType: VariantBinary, want: []byte{0x00, 0xff, 0x10}},
		{expression: "CAST('short' AS VARIANT)", value: "FXNob3J0", wantType: VariantString, want: "short"},
		{expression: "CAST('' AS VARIANT)", value: "AQ==", wantType: VariantString, want: ""},
		{
			expression: "CAST('0123456789012345678901234567890123456789012345678901234567890123456789' AS VARIANT)",
			value:      "QEYAAAAwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5",
			wantType:   VariantString,
			want:       "0123456789012345678901234567890123456789012345678901234567890123456789",
		},
		{
			expression: "CAST(UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' AS VARIANT)",
			value:      "UBIVH9J1hhHpj54qhuQIWlk=",
			wantType:   VariantUUID,
			want:       "12151fd2-7586-11e9-8f9e-2a86e4085a59",
		},
		{expression: "CAST(JSON '[]' AS VARIANT)", value: "AwAA", wantType: VariantArray, want: []interface{}{}},
		{expression: "CAST(ARRAY[1, NULL, 3] AS VARIANT)", value: "AwMABQYLFAEAAAAAFAMAAAA=", wantType: VariantArray, want: []interface{}{int64(1), nil, int64(3)}},
		{
			expression: "CAST(ARRAY[CAST(UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' AS VARIANT), CAST(X'01' AS VARIANT), CAST(DECIMAL '1.50' AS VARIANT)] AS VARIANT)",
			value:      "AwMAERcdUBIVH9J1hhHpj54qhuQIWlk8AQAAAAEgApYAAAA=",
			wantType:   VariantArray,
			want:       []interface{}{"12151fd2-7586-11e9-8f9e-2a86e4085a59", []byte{0x01}, Numeric("1.50")},
		},
		{expression: "CAST(JSON '{}' AS VARIANT)", value: "AgAA", wantType: VariantObject, want: map[string]interface{}{}},
		{
			expression: "CAST(CAST(ROW(1, 'x', DATE '2020-01-02') AS ROW(id INTEGER, name VARCHAR, day DATE)) AS VARIANT)",
			metadata:   "EQMAAwUJZGF5aWRuYW1l",
			value:      "AgMAAQIABQoMLFdHAAAUAQAAAAV4",
			wantType:   VariantObject,
			want:       map[string]interface{}{"id": int64(1), "name": "x", "day": time.Date(2020, 1, 2, 0, 0, 0, 0, location)},
		},
		{
			expression: `CAST(JSON '{"b": 1, "a": {"d": null, "c": [true, "x"]}}' AS VARIANT)`,
			metadata:   "EQQAAQIDBGFiY2Q=",
			value:      "AgIAAQAQFQICAgMACAkDAgABAwQFeAAUAQAAAA==",
			wantType:   VariantObject,
			want:       map[string]interface{}{"a": map[string]interface{}{"c": []interface{}{true, "x"}, "d": nil}, "b": int64(1)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.expression, func(t *testing.T) {
			t.Parallel()
			metadata := tc.metadata
			if metadata == "" {
				metadata = "AQAA"
			}
			variant := decodeVariantVector(t, metadata, tc.value, location)

			assert.True(t, variant.Valid)
			assert.Equal(t, tc.wantType, variant.Type())
			assert.Equal(t, tc.want, variant.Value())
		})
	}
}

func TestVariantSQLNull(t *testing.T) {
	t.Parallel()
	converted, err := decodeJSON(t, typeSignature{RawType: "variant"}, `null`, time.UTC)
	require.NoError(t, err)
	assert.Nil(t, converted)

	var variant Variant
	require.NoError(t, variant.Scan(nil))
	assert.False(t, variant.Valid)
	assert.Equal(t, VariantNull, variant.Type())
	assert.Nil(t, variant.Value())
	assert.Equal(t, "null", variant.String())

	variantNull := decodeVariantVector(t, "AQAA", "AA==", time.UTC)
	assert.True(t, variantNull.Valid, "a VARIANT null is not SQL NULL")
	assert.Equal(t, VariantNull, variantNull.Type())
}

func TestVariantScan(t *testing.T) {
	t.Parallel()
	source := decodeVariantVector(t, "AQAA", "FXNob3J0", time.UTC)
	var variant Variant
	require.NoError(t, variant.Scan(source))
	assert.Equal(t, source, variant)

	assert.EqualError(t, variant.Scan("short"), "trino: cannot convert short (string) to Variant")
}

func TestVariantTypeString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "TIMESTAMP_UTC_NANOS", VariantTimestampUTCNanos.String())
	assert.Equal(t, "ARRAY", VariantArray.String())
	assert.Equal(t, "VariantType(42)", VariantType(42).String())
}

func TestDecodeVariantRejectsMalformedValues(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		encoded string
		wantErr string
	}{
		{name: "not an object", encoded: `"AA=="`, wantErr: "expected a variant object, got a string"},
		{name: "no metadata", encoded: `{"value": "AA=="}`, wantErr: "variant has no metadata"},
		{name: "no value", encoded: `{"metadata": "AQAA"}`, wantErr: "variant has no value"},
		{
			name:    "value not base64",
			encoded: `{"metadata": "AQAA", "value": "!"}`,
			wantErr: "variant value: cannot decode base64 string: illegal base64 data at input byte 0",
		},
		{
			name:    "metadata not a string",
			encoded: `{"metadata": 1, "value": "AA=="}`,
			wantErr: "variant metadata: expected a base64 string, got a number",
		},
		{name: "empty value", encoded: encodedVariant([]byte{1, 0, 0}, nil), wantErr: "variant value is empty"},
		{name: "metadata version", encoded: encodedVariant([]byte{2, 0, 0}, []byte{0}), wantErr: "unsupported variant metadata version 2"},
		{name: "metadata truncated", encoded: encodedVariant([]byte{1, 2, 0}, []byte{0}), wantErr: "variant metadata is truncated"},
		{
			name:    "metadata offsets decreasing",
			encoded: encodedVariant([]byte{1, 2, 0, 2, 1, 'a', 'b'}, []byte{0}),
			wantErr: "variant metadata has an invalid offset for key 2",
		},
		{name: "unknown primitive type", encoded: encodedVariant([]byte{1, 0, 0}, []byte{21 << 2}), wantErr: "unknown variant primitive type 21"},
		{name: "truncated int64", encoded: encodedVariant([]byte{1, 0, 0}, []byte{6 << 2, 1, 2}), wantErr: "variant value is truncated"},
		{name: "truncated string length", encoded: encodedVariant([]byte{1, 0, 0}, []byte{16 << 2, 1}), wantErr: "variant value is truncated"},
		{name: "truncated string", encoded: encodedVariant([]byte{1, 0, 0}, []byte{16 << 2, 5, 0, 0, 0, 'a'}), wantErr: "variant value is truncated"},
		{name: "trailing bytes", encoded: encodedVariant([]byte{1, 0, 0}, []byte{0, 0}), wantErr: "variant value has 2 bytes but encodes 1"},
		{name: "array count too large", encoded: encodedVariant([]byte{1, 0, 0}, []byte{3, 200, 0}), wantErr: "variant value is truncated"},
		{
			name:    "array element out of range",
			encoded: encodedVariant([]byte{1, 0, 0}, []byte{3, 1, 2, 1, 0}),
			wantErr: "variant element 0 has invalid offsets 2 and 1",
		},
		{
			name:    "object field ID not in metadata",
			encoded: encodedVariant([]byte{1, 0, 0}, []byte{2, 1, 0, 0, 1, 0}),
			wantErr: "variant field ID 0 is not in the metadata dictionary of 0 keys",
		},
		{name: "nested too deeply", encoded: encodedVariant([]byte{1, 0, 0}, deeplyNestedArray(variantMaxDepth+1)), wantErr: "variant value nests deeper than 10000 levels"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeJSON(t, typeSignature{RawType: "variant"}, tc.encoded, time.UTC)
			assert.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestClientCapabilitiesIncludeVariantBinary(t *testing.T) {
	t.Parallel()
	capabilities := strings.Split(clientCapabilities, commaSeparator)

	assert.Contains(t, capabilities, "VARIANT")
	assert.Contains(t, capabilities, "VARIANT_BINARY")
}

// TestVariantColumnScansThroughTheWireFormat serves VARIANT values the way
// Trino 483 does once VARIANT_BINARY is announced, on their own and nested
// in an ARRAY, a MAP and a ROW.
func TestVariantColumnScansThroughTheWireFormat(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	variantType := typeSignature{RawType: "variant", Arguments: []typeArgument{}}
	variantArgument := typeArgument{Kind: KIND_TYPE, Value: jsontext.Value(`{"rawType":"variant","arguments":[]}`)}
	columns := []queryColumn{
		{Name: "v", Type: "variant", TypeSignature: variantType},
		{Name: "a", Type: "array(variant)", TypeSignature: typeSignature{RawType: "array", Arguments: []typeArgument{variantArgument}}},
		{
			Name: "m",
			Type: "map(varchar, variant)",
			TypeSignature: typeSignature{
				RawType: "map",
				Arguments: []typeArgument{
					{Kind: KIND_TYPE, Value: jsontext.Value(`{"rawType":"varchar","arguments":[]}`)},
					variantArgument,
				},
			},
		},
		rowColumn("r", "row(x variant)", namedField("x", variantType)),
	}
	object := jsontext.Value(`{"metadata":"EQIAAQJhYg==","value":"AgIAAQAFChQCAAAAFAEAAAA="}`)
	double := jsontext.Value(`{"metadata":"AQAA","value":"HAAAAAAAAPg/"}`)
	variantNull := jsontext.Value(`{"metadata":"AQAA","value":"AA=="}`)
	date := jsontext.Value(`{"metadata":"AQAA","value":"LFdHAAA="}`)
	uuid := jsontext.Value(`{"metadata":"AQAA","value":"UBIVH9J1hhHpj54qhuQIWlk="}`)
	fc.respond(statementPage(), columnsPage(columns, [][]any{
		{object, []any{double, variantNull, nil}, map[string]any{"k": date}, []any{uuid}},
		{variantNull, nil, nil, []any{nil}},
		{nil, nil, nil, nil},
	}))
	db := fc.open(t, "?timezone=Asia%2FTokyo")
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)

	rows, err := db.Query("SELECT v, a, m, r")
	require.NoError(t, err)
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	assert.Equal(t, "VARIANT", columnTypes[0].DatabaseTypeName())
	assert.Equal(t, reflect.TypeOf(Variant{}), columnTypes[0].ScanType())
	assert.Equal(t, "ARRAY(VARIANT)", columnTypes[1].DatabaseTypeName())
	assert.Equal(t, reflect.TypeOf(NullSlice[Variant]{}), columnTypes[1].ScanType())

	require.True(t, rows.Next())
	var value Variant
	var array NullSlice[sql.NullString]
	var arrayElements interface{}
	var mapValue NullMap[string, interface{}]
	var row Row
	require.NoError(t, rows.Scan(&value, &array, &mapValue, &row))
	var genericArray NullSlice[Variant]
	require.NoError(t, rows.Scan(new(Variant), &genericArray, new(NullMap[string, interface{}]), new(Row)))
	require.True(t, genericArray.Valid)
	require.Len(t, genericArray.Slice, 3)
	assert.Equal(t, 1.5, genericArray.Slice[0].Value())
	assert.Equal(t, VariantNull, genericArray.Slice[1].Type())
	assert.False(t, genericArray.Slice[2].Valid, "an SQL NULL element")
	assert.True(t, value.Valid)
	assert.Equal(t, VariantObject, value.Type())
	assert.Equal(t, map[string]interface{}{"a": int64(2), "b": int64(1)}, value.Value())
	assert.Equal(t, `{"a":2,"b":1}`, value.String())
	assert.Equal(t, NullSlice[sql.NullString]{
		Slice: []sql.NullString{{String: "1.5", Valid: true}, {String: "null", Valid: true}, {}},
		Valid: true,
	}, array, "ARRAY(VARIANT) still scans into the JSON text of each element")
	require.NoError(t, rows.Scan(new(Variant), &arrayElements, new(NullMap[string, interface{}]), new(Row)))
	require.IsType(t, []interface{}{}, arrayElements)
	elements := arrayElements.([]interface{})
	require.Len(t, elements, 3)
	assert.Equal(t, 1.5, elements[0].(Variant).Value())
	assert.Equal(t, VariantNull, elements[1].(Variant).Type())
	assert.Nil(t, elements[2], "an SQL NULL element")
	require.IsType(t, Variant{}, mapValue.Map["k"])
	assert.Equal(t, time.Date(2020, 1, 2, 0, 0, 0, 0, tokyo), mapValue.Map["k"].(Variant).Value(), "a date is in the connection's time zone")
	field, ok := row.Field("x")
	require.True(t, ok)
	require.IsType(t, Variant{}, field)
	assert.Equal(t, VariantUUID, field.(Variant).Type())
	assert.Equal(t, "12151fd2-7586-11e9-8f9e-2a86e4085a59", field.(Variant).Value())

	var text sql.NullString
	assert.ErrorContains(t, rows.Scan(&text, new(interface{}), new(interface{}), new(interface{})), "unsupported Scan",
		"database/sql only converts strings and numbers into a string, so a VARIANT column no longer scans into one")

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&value, &array, &mapValue, &row))
	assert.True(t, value.Valid, "a VARIANT null is not SQL NULL")
	assert.Equal(t, VariantNull, value.Type())
	assert.Nil(t, value.Value())
	assert.False(t, array.Valid)
	assert.False(t, mapValue.Valid)
	assert.Equal(t, Row{names: []string{"x"}, values: []interface{}{nil}, Valid: true}, row)

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&value, &array, &mapValue, &row))
	assert.False(t, value.Valid, "SQL NULL")
	assert.False(t, row.Valid)

	assert.False(t, rows.Next())
	require.NoError(t, rows.Err())

	requests := fc.capturedRequests()
	require.NotEmpty(t, requests)
	assert.Contains(t, strings.Split(requests[0].header.Get(trinoClientCapabilitiesHeader), commaSeparator), "VARIANT_BINARY")
}

func TestVariantColumnRejectsMalformedValues(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	columns := []queryColumn{{Name: "v", Type: "variant", TypeSignature: typeSignature{RawType: "variant", Arguments: []typeArgument{}}}}
	fc.respond(statementPage(), columnsPage(columns, [][]any{{jsontext.Value(`{"metadata":"AQAA","value":"GAE="}`)}}))
	db := fc.open(t, "")

	_, err := db.Query("SELECT v")

	assert.ErrorContains(t, err, `trino: row 0: column "v": variant value is truncated`)
}

// FuzzNewVariant checks that whatever newVariant accepts can be read without
// panicking, since reading relies on newVariant having validated the bytes.
func FuzzNewVariant(f *testing.F) {
	content, err := os.ReadFile("testdata/variant_vectors.json")
	require.NoError(f, err)
	var vectors []variantVector
	require.NoError(f, json.Unmarshal(content, &vectors))
	for _, vector := range vectors {
		metadata, err := base64.StdEncoding.DecodeString(vector.Metadata)
		require.NoError(f, err)
		value, err := base64.StdEncoding.DecodeString(vector.Value)
		require.NoError(f, err)
		f.Add(metadata, value)
	}
	f.Fuzz(func(t *testing.T, metadata, value []byte) {
		variant, err := newVariant(metadata, value, time.UTC)
		if err != nil {
			return
		}
		_ = variant.Value()
		_ = variant.String()
	})
}

func decodeVariantVector(t *testing.T, metadata, value string, location *time.Location) Variant {
	t.Helper()
	converted, err := decodeJSON(t, typeSignature{RawType: "variant"}, `{"metadata":"`+metadata+`","value":"`+value+`"}`, location)
	require.NoError(t, err)
	variant, ok := converted.(Variant)
	require.True(t, ok, "decoded %T", converted)
	return variant
}

// encodedVariant returns the JSON object the server sends for a VARIANT value.
func encodedVariant(metadata, value []byte) string {
	return `{"metadata":"` + base64.StdEncoding.EncodeToString(metadata) + `","value":"` + base64.StdEncoding.EncodeToString(value) + `"}`
}

// deeplyNestedArray encodes depth arrays nested in each other around a null,
// each with one element and 4-byte offsets.
func deeplyNestedArray(depth int) []byte {
	value := []byte{0}
	for i := 0; i < depth; i++ {
		size := len(value)
		header := []byte{0b11<<2 | 3, 1, 0, 0, 0, 0, byte(size), byte(size >> 8), byte(size >> 16), byte(size >> 24)}
		value = append(header, value...)
	}
	return value
}

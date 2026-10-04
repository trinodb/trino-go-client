package trino

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NullSlice represents an ARRAY value that may be null. Each element is
// scanned into T, so arrays of any depth are scanned by nesting it, like
// NullSlice[NullSlice[sql.NullInt64]] for an ARRAY(ARRAY(BIGINT)). A NULL
// array, at any depth, scans with Valid set to false and a nil Slice.
//
// T can be:
//   - bool, string, int64, int32, int16, int8, int, float64, float32 or
//     time.Time, for which a NULL element is an error
//   - []byte, map[string]interface{}, []interface{} or interface{}, which
//     hold nil for a NULL element
//   - sql.NullBool, sql.NullString, sql.NullInt64, sql.NullInt32,
//     sql.NullInt16, sql.NullFloat64, sql.NullTime, NullTime, NullBinary or
//     NullMap, which keep a NULL element as not valid
//   - NullSlice, NullMapOf or NullRow, for nested arrays, maps and rows
//   - any other type whose pointer implements sql.Scanner, like Row; its
//     Scan receives the element in the shape the JSON response used, like a
//     json.Number for a number, or converted the way a plain column would be
//     when the array contains a ROW (see Row)
//
// []byte elements are decoded from the base64 form VARBINARY values travel
// in. Elements without a time zone are interpreted in Location, or in
// time.Local when Location is nil; set it to the zone of the connection,
// which the server used to produce them. Location is passed down to nested
// NullSlice and NullMapOf values.
type NullSlice[T any] struct {
	Slice    []T
	Valid    bool
	Location *time.Location
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice[T]) Scan(value interface{}) error {
	return wrapScanError(s.scanValue(value, s.Location))
}

func (s *NullSlice[T]) scanValue(value interface{}, location *time.Location) error {
	if value == nil {
		s.Slice, s.Valid = nil, false
		return nil
	}
	values, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("cannot convert %v (%T) to %s", value, value, reflect.TypeFor[[]T]())
	}
	slice := make([]T, len(values))
	for i, v := range values {
		if err := scanElement(&slice[i], v, location); err != nil {
			return fmt.Errorf("element %d: %w", i, err)
		}
	}
	s.Slice, s.Valid = slice, true
	return nil
}

// NullMapOf represents a MAP value that may be null, with keys scanned into
// K and values into V. V can be any element type NullSlice accepts. K can be
// string, bool, any of the integer and floating point types NullSlice
// accepts, time.Time, or any other comparable type whose pointer implements
// sql.Scanner, which receives the key as a string. A NULL map scans with
// Valid set to false and a nil Map. Location is used and passed down the
// same way as in NullSlice.
//
// It is not called NullMap because that name already belongs to the map
// scanner that predates generics.
type NullMapOf[K comparable, V any] struct {
	Map      map[K]V
	Valid    bool
	Location *time.Location
}

// Scan implements the sql.Scanner interface.
func (m *NullMapOf[K, V]) Scan(value interface{}) error {
	return wrapScanError(m.scanValue(value, m.Location))
}

func (m *NullMapOf[K, V]) scanValue(value interface{}, location *time.Location) error {
	if value == nil {
		m.Map, m.Valid = nil, false
		return nil
	}
	values, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("cannot convert %v (%T) to %s", value, value, reflect.TypeFor[map[K]V]())
	}
	result := make(map[K]V, len(values))
	for rawKey, rawValue := range values {
		var key K
		if err := scanElement(&key, mapKeyValue(&key, rawKey), location); err != nil {
			return fmt.Errorf("key %q: %w", rawKey, err)
		}
		var v V
		if err := scanElement(&v, rawValue, location); err != nil {
			return fmt.Errorf("value for key %q: %w", rawKey, err)
		}
		result[key] = v
	}
	m.Map, m.Valid = result, true
	return nil
}

// mapKeyValue turns a map key, which the JSON response always sends as a
// string, into the shape an element of the same type would arrive in.
func mapKeyValue(dest interface{}, key string) interface{} {
	switch dest.(type) {
	case *bool, *sql.NullBool:
		if b, err := strconv.ParseBool(key); err == nil {
			return b
		}
	case *int64, *int32, *int16, *int8, *int, *sql.NullInt64, *sql.NullInt32, *sql.NullInt16, *float64, *float32, *sql.NullFloat64:
		return json.Number(key)
	}
	return key
}

// NullRow represents a ROW value that may be null, scanned into the struct
// T. A NULL row scans with Valid set to false and a zero Row.
//
// Each ROW field is stored in the exported struct field it maps to:
//   - a struct field's key is the name in its `trino:"name"` tag, or its Go
//     name when it has no tag; a field tagged `trino:"-"` is skipped
//   - a ROW field maps to the struct field whose key equals its name or,
//     when there is none, to the only struct field whose key equals it
//     ignoring case
//   - an anonymous ROW field is named field<i>, where i is its zero-based
//     position (see Row), so it maps to a struct field called Field0,
//     Field1 and so on, or one tagged `trino:"field0"`
//
// A ROW field that maps to no struct field, or to the same struct field as
// another ROW field, is an error, so no value is silently dropped. A struct
// field no ROW field maps to keeps its zero value. Struct fields can be of
// any element type NullSlice accepts, including NullRow for a nested ROW.
// Embedded structs are not flattened: they are matched like any other
// field, by their type name.
type NullRow[T any] struct {
	Row   T
	Valid bool
}

// Scan implements the sql.Scanner interface.
func (r *NullRow[T]) Scan(value interface{}) error {
	return wrapScanError(r.scanValue(value, nil))
}

func (r *NullRow[T]) scanValue(value interface{}, location *time.Location) error {
	var result T
	if value == nil {
		r.Row, r.Valid = result, false
		return nil
	}
	row, ok := value.(Row)
	if !ok {
		return fmt.Errorf("cannot convert %v (%T) to %s", value, value, reflect.TypeFor[T]())
	}
	if !row.Valid {
		r.Row, r.Valid = result, false
		return nil
	}
	target := reflect.ValueOf(&result).Elem()
	fields, err := rowStructFieldsOf(target.Type())
	if err != nil {
		return err
	}
	assigned := make(map[int]string, row.Len())
	for i := range row.Len() {
		name := row.Name(i)
		index, err := fields.lookup(name)
		if err != nil {
			return err
		}
		if previous, ok := assigned[index]; ok {
			return fmt.Errorf("row fields %q and %q both map to %s.%s", previous, name, target.Type(), target.Type().Field(index).Name)
		}
		assigned[index] = name
		if err := scanElement(target.Field(index).Addr().Interface(), row.Value(i), location); err != nil {
			return fmt.Errorf("field %q: %w", name, err)
		}
	}
	r.Row, r.Valid = result, true
	return nil
}

// elementScanner is implemented by the generic scanners, so a nested one
// receives the outer Location and reports errors without its own prefix.
type elementScanner interface {
	scanValue(value interface{}, location *time.Location) error
}

func wrapScanError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("trino: %w", err)
}

// scanElement stores one ARRAY element, MAP key or value, or ROW field in
// dest. The value is either in the shape the JSON response used, or already
// converted by convertRows when the column contains a ROW, so both shapes
// are accepted.
func scanElement(dest interface{}, value interface{}, location *time.Location) error {
	switch d := dest.(type) {
	case elementScanner:
		return d.scanValue(value, location)
	case *interface{}:
		*d = value
		return nil
	case *bool:
		v, err := scanNullBool(value)
		return setNotNull(d, v.Bool, v.Valid, err)
	case *sql.NullBool:
		v, err := scanNullBool(value)
		return setNullable(d, v, err)
	case *string:
		v, err := scanNullString(value)
		return setNotNull(d, v.String, v.Valid, err)
	case *sql.NullString:
		v, err := scanNullString(value)
		return setNullable(d, v, err)
	case *int64:
		v, err := elementInt64(value)
		return setNotNull(d, v.Int64, v.Valid, err)
	case *int32:
		return setInteger(d, value)
	case *int16:
		return setInteger(d, value)
	case *int8:
		return setInteger(d, value)
	case *int:
		return setInteger(d, value)
	case *sql.NullInt64:
		v, err := elementInt64(value)
		return setNullable(d, v, err)
	case *sql.NullInt32:
		v, err := elementInteger[int32](value)
		return setNullable(d, sql.NullInt32{Int32: v.V, Valid: v.Valid}, err)
	case *sql.NullInt16:
		v, err := elementInteger[int16](value)
		return setNullable(d, sql.NullInt16{Int16: v.V, Valid: v.Valid}, err)
	case *float64:
		v, err := elementFloat64(value)
		return setNotNull(d, v.Float64, v.Valid, err)
	case *float32:
		v, err := elementFloat64(value)
		return setNotNull(d, float32(v.Float64), v.Valid, err)
	case *sql.NullFloat64:
		v, err := elementFloat64(value)
		return setNullable(d, v, err)
	case *time.Time:
		v, err := elementTime(value, location)
		return setNotNull(d, v.Time, v.Valid, err)
	case *NullTime:
		v, err := elementTime(value, location)
		return setNullable(d, v, err)
	case *sql.NullTime:
		v, err := elementTime(value, location)
		return setNullable(d, sql.NullTime{Time: v.Time, Valid: v.Valid}, err)
	case *[]byte:
		v, err := elementBytes(value)
		return setNullable(d, v.Bytes, err)
	case *NullBinary:
		v, err := elementBytes(value)
		return setNullable(d, v, err)
	case *map[string]interface{}:
		if err := validateMap(value); err != nil {
			return err
		}
		*d, _ = value.(map[string]interface{})
		return nil
	case *[]interface{}:
		if err := validateSlice(value); err != nil {
			return err
		}
		*d, _ = value.([]interface{})
		return nil
	case *NullMap:
		// NullMap.Scan leaves anything that is not a map invalid instead of failing
		if err := validateMap(value); err != nil {
			return err
		}
		return d.Scan(value)
	case sql.Scanner:
		return d.Scan(value)
	default:
		return fmt.Errorf("unsupported element type %s", reflect.TypeOf(dest).Elem())
	}
}

func setNotNull[V any](dest *V, value V, valid bool, err error) error {
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("cannot convert NULL to %T, use a nullable element type", value)
	}
	*dest = value
	return nil
}

func setNullable[V any](dest *V, value V, err error) error {
	if err != nil {
		return err
	}
	*dest = value
	return nil
}

type integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

func setInteger[N integer](dest *N, value interface{}) error {
	v, err := elementInteger[N](value)
	return setNotNull(dest, v.V, v.Valid, err)
}

func elementInteger[N integer](value interface{}) (sql.Null[N], error) {
	v, err := elementInt64(value)
	if err != nil {
		return sql.Null[N]{}, err
	}
	narrowed := N(v.Int64)
	if int64(narrowed) != v.Int64 {
		return sql.Null[N]{}, fmt.Errorf("value %d overflows %T", v.Int64, narrowed)
	}
	return sql.Null[N]{V: narrowed, Valid: v.Valid}, nil
}

func elementInt64(value interface{}) (sql.NullInt64, error) {
	if v, ok := value.(int64); ok {
		return sql.NullInt64{Int64: v, Valid: true}, nil
	}
	return scanNullInt64(value)
}

func elementFloat64(value interface{}) (sql.NullFloat64, error) {
	if v, ok := value.(float64); ok {
		return sql.NullFloat64{Float64: v, Valid: true}, nil
	}
	return scanNullFloat64(value)
}

func elementTime(value interface{}, location *time.Location) (NullTime, error) {
	if v, ok := value.(time.Time); ok {
		return NullTime{Time: v, Valid: true}, nil
	}
	if location == nil {
		location = time.Local
	}
	return scanNullTime(value, location)
}

func elementBytes(value interface{}) (NullBinary, error) {
	if v, ok := value.([]byte); ok {
		return NullBinary{Bytes: v, Valid: true}, nil
	}
	return scanNullBytes(value)
}

// rowStructFields holds the keys NullRow matches ROW field names against,
// with the index of the struct field each one belongs to.
type rowStructFields struct {
	structType reflect.Type
	keys       []string
	indexes    []int
}

var rowStructFieldsCache sync.Map

func rowStructFieldsOf(structType reflect.Type) (*rowStructFields, error) {
	if cached, ok := rowStructFieldsCache.Load(structType); ok {
		return cached.(*rowStructFields), nil
	}
	if structType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("NullRow type parameter must be a struct, got %s", structType)
	}
	fields := &rowStructFields{structType: structType}
	for i := range structType.NumField() {
		field := structType.Field(i)
		if !field.IsExported() {
			continue
		}
		key := field.Name
		if tag, ok := field.Tag.Lookup("trino"); ok {
			if tag == "-" {
				continue
			}
			key = tag
		}
		fields.keys = append(fields.keys, key)
		fields.indexes = append(fields.indexes, i)
	}
	cached, _ := rowStructFieldsCache.LoadOrStore(structType, fields)
	return cached.(*rowStructFields), nil
}

func (f *rowStructFields) lookup(name string) (int, error) {
	for i, key := range f.keys {
		if key == name {
			return f.indexes[i], nil
		}
	}
	found := -1
	for i, key := range f.keys {
		if !strings.EqualFold(key, name) {
			continue
		}
		if found != -1 {
			return 0, fmt.Errorf("row field %q matches both %s and %s ignoring case", name, f.fieldName(found), f.fieldName(i))
		}
		found = i
	}
	if found == -1 {
		return 0, fmt.Errorf("row field %q has no matching field in %s", name, f.structType)
	}
	return f.indexes[found], nil
}

func (f *rowStructFields) fieldName(i int) string {
	return f.structType.String() + "." + f.structType.Field(f.indexes[i]).Name
}

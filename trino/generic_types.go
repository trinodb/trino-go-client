package trino

import (
	"database/sql"
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
//     sql.NullInt16, sql.NullFloat64, sql.NullTime, NullTime or NullBinary,
//     which keep a NULL element as not valid
//   - NullSlice, NullMap or NullRow, for nested arrays, maps and rows
//   - any other type whose pointer implements sql.Scanner, like Row or
//     Variant; its Scan receives the element converted the way a plain
//     column of the element type would be
//
// An element converts into T when it holds that type, like an int64 for
// BIGINT and the narrower integer types, a time.Time for TIMESTAMP and a
// []byte for VARBINARY. An integer element also converts into the floating
// point types, and so does a DECIMAL element, a string otherwise. Elements
// arrive in the zone of the connection, so Location is only used to parse
// the keys of a nested NullMap and is passed down to nested NullSlice and
// NullMap values.
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

// NullMap represents a MAP value that may be null, with keys scanned into
// K and values into V. V can be any element type NullSlice accepts. K can be
// string, bool, any of the integer and floating point types NullSlice
// accepts, time.Time, or any other comparable type whose pointer implements
// sql.Scanner, which receives the key as a string. A NULL map scans with
// Valid set to false and a nil Map.
//
// The server sends the keys as strings, which NullMap parses into K. Keys
// without a time zone, like those of a MAP(DATE, ...), are interpreted in
// Location, or in time.Local when Location is nil; set it to the zone of the
// connection, which the server used to produce them. Location is passed
// down to nested NullSlice and NullMap values.
type NullMap[K comparable, V any] struct {
	Map      map[K]V
	Valid    bool
	Location *time.Location
}

// Scan implements the sql.Scanner interface.
func (m *NullMap[K, V]) Scan(value interface{}) error {
	return wrapScanError(m.scanValue(value, m.Location))
}

func (m *NullMap[K, V]) scanValue(value interface{}, location *time.Location) error {
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
		keyValue, err := mapKeyValue(&key, rawKey, location)
		if err != nil {
			return fmt.Errorf("key %q: %w", rawKey, err)
		}
		if err := scanElement(&key, keyValue, location); err != nil {
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
// string, into the value an element of the type of dest would be.
func mapKeyValue(dest interface{}, key string, location *time.Location) (interface{}, error) {
	switch dest.(type) {
	case *bool, *sql.NullBool:
		return parsedKey(strconv.ParseBool(key))
	case *int64, *int32, *int16, *int8, *int, *sql.NullInt64, *sql.NullInt32, *sql.NullInt16:
		return parsedKey(strconv.ParseInt(key, 10, 64))
	case *float64, *float32, *sql.NullFloat64:
		return parsedKey(strconv.ParseFloat(key, 64))
	case *time.Time, *NullTime, *sql.NullTime:
		if location == nil {
			location = time.Local
		}
		return parsedKey(parseTime(key, location))
	}
	return key, nil
}

func parsedKey[V any](value V, err error) (interface{}, error) {
	if err != nil {
		return nil, err
	}
	return value, nil
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
// dest. The value was decoded the way a plain column of its type is, or
// parsed from a MAP key by mapKeyValue.
func scanElement(dest interface{}, value interface{}, location *time.Location) error {
	switch d := dest.(type) {
	case elementScanner:
		return d.scanValue(value, location)
	case *interface{}:
		*d = value
		return nil
	case *bool:
		v, err := element[bool](value)
		return setNotNull(d, v, err)
	case *sql.NullBool:
		v, err := element[bool](value)
		return setNullable(d, sql.NullBool{Bool: v.V, Valid: v.Valid}, err)
	case *string:
		v, err := elementString(value)
		return setNotNull(d, v, err)
	case *sql.NullString:
		v, err := elementString(value)
		return setNullable(d, sql.NullString{String: v.V, Valid: v.Valid}, err)
	case *int64:
		v, err := element[int64](value)
		return setNotNull(d, v, err)
	case *int32:
		return setInteger(d, value)
	case *int16:
		return setInteger(d, value)
	case *int8:
		return setInteger(d, value)
	case *int:
		return setInteger(d, value)
	case *sql.NullInt64:
		v, err := element[int64](value)
		return setNullable(d, sql.NullInt64{Int64: v.V, Valid: v.Valid}, err)
	case *sql.NullInt32:
		v, err := elementInteger[int32](value)
		return setNullable(d, sql.NullInt32{Int32: v.V, Valid: v.Valid}, err)
	case *sql.NullInt16:
		v, err := elementInteger[int16](value)
		return setNullable(d, sql.NullInt16{Int16: v.V, Valid: v.Valid}, err)
	case *float64:
		v, err := elementFloat64(value)
		return setNotNull(d, v, err)
	case *float32:
		v, err := elementFloat64(value)
		return setNotNull(d, sql.Null[float32]{V: float32(v.V), Valid: v.Valid}, err)
	case *sql.NullFloat64:
		v, err := elementFloat64(value)
		return setNullable(d, sql.NullFloat64{Float64: v.V, Valid: v.Valid}, err)
	case *time.Time:
		v, err := element[time.Time](value)
		return setNotNull(d, v, err)
	case *NullTime:
		v, err := element[time.Time](value)
		return setNullable(d, NullTime{Time: v.V, Valid: v.Valid}, err)
	case *sql.NullTime:
		v, err := element[time.Time](value)
		return setNullable(d, sql.NullTime{Time: v.V, Valid: v.Valid}, err)
	case *[]byte:
		return setValue(d, value)
	case *NullBinary:
		v, err := element[[]byte](value)
		return setNullable(d, NullBinary{Bytes: v.V, Valid: v.Valid}, err)
	case *map[string]interface{}:
		return setValue(d, value)
	case *[]interface{}:
		return setValue(d, value)
	case sql.Scanner:
		return d.Scan(value)
	default:
		return fmt.Errorf("unsupported element type %s", reflect.TypeOf(dest).Elem())
	}
}

// setValue stores value, which must be a V or nil, in dest, which holds the
// zero V for nil.
func setValue[V any](dest *V, value interface{}) error {
	v, err := element[V](value)
	return setNullable(dest, v.V, err)
}

func setNotNull[V any](dest *V, value sql.Null[V], err error) error {
	if err != nil {
		return err
	}
	if !value.Valid {
		return fmt.Errorf("cannot convert NULL to %s, use a nullable element type", reflect.TypeFor[V]())
	}
	*dest = value.V
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
	return setNotNull(dest, v, err)
}

func elementInteger[N integer](value interface{}) (sql.Null[N], error) {
	v, err := element[int64](value)
	if err != nil {
		return sql.Null[N]{}, err
	}
	narrowed := N(v.V)
	if int64(narrowed) != v.V {
		return sql.Null[N]{}, fmt.Errorf("value %d overflows %T", v.V, narrowed)
	}
	return sql.Null[N]{V: narrowed, Valid: v.Valid}, nil
}

// element returns value, which must be a V or nil, as a sql.Null[V].
func element[V any](value interface{}) (sql.Null[V], error) {
	if value == nil {
		return sql.Null[V]{}, nil
	}
	v, ok := value.(V)
	if !ok {
		return sql.Null[V]{}, fmt.Errorf("cannot convert %v (%T) to %s", value, value, reflect.TypeFor[V]())
	}
	return sql.Null[V]{V: v, Valid: true}, nil
}

func elementString(value interface{}) (sql.Null[string], error) {
	// An ARRAY(VARIANT) element, which scans into the JSON text a VARIANT
	// column held before the driver decoded VARIANT values.
	if variant, ok := value.(Variant); ok {
		return sql.Null[string]{V: variant.String(), Valid: true}, nil
	}
	return element[string](value)
}

func elementFloat64(value interface{}) (sql.Null[float64], error) {
	switch v := value.(type) {
	case int64:
		return sql.Null[float64]{V: float64(v), Valid: true}, nil
	case string:
		// A DECIMAL or NUMBER element.
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return sql.Null[float64]{}, fmt.Errorf("cannot convert %v (%T) to float64: %w", value, value, err)
		}
		return sql.Null[float64]{V: parsed, Valid: true}, nil
	}
	return element[float64](value)
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

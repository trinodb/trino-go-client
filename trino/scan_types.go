package trino

import (
	"database/sql"
	"reflect"
)

// containerScanTypes holds the NullSlice and NullMap instantiations
// ColumnTypeScanType reports for ARRAY and MAP columns.
//
// Go instantiates generic types only at compile time, and reflect cannot
// build NullSlice[T] from the reflect.Type of T, so every instantiation is
// listed ahead of time. A generic function cannot generate them by nesting
// NullSlice[T] in itself to some depth either: the compiler rejects that as
// an instantiation cycle. Each instantiation adds code to every binary using
// the driver, a few kilobytes for a NullMap, so the table covers the
// shapes the hand-written scanners covered rather than every combination:
//   - arrays of up to three dimensions of any scalar scan type, including
//     Row and interface{}
//   - maps from any comparable scalar scan type to a scalar scan type
//   - arrays of such maps
//
// When a column nests beyond the table, getScanType reports the deepest
// instantiation the table covers, with interface{} for the values below
// that level, holding them as an interface{} element of NullSlice would. A
// key type that is not comparable, like []byte, is reported as interface{}.
var containerScanTypes = newScanTypeTable()

type scanTypeTable struct {
	slices map[reflect.Type]reflect.Type
	maps   map[mapScanTypeKey]reflect.Type
	keys   map[reflect.Type]bool
}

type mapScanTypeKey struct {
	key   reflect.Type
	value reflect.Type
}

var anyScanType = reflect.TypeFor[interface{}]()

func (t *scanTypeTable) slice(element reflect.Type) (reflect.Type, bool) {
	sliceType, ok := t.slices[element]
	return sliceType, ok
}

func (t *scanTypeTable) mapOf(key, value reflect.Type) (reflect.Type, bool) {
	if !t.keys[key] {
		key = anyScanType
	}
	mapType, ok := t.maps[mapScanTypeKey{key, value}]
	return mapType, ok
}

func newScanTypeTable() *scanTypeTable {
	t := &scanTypeTable{
		slices: make(map[reflect.Type]reflect.Type),
		maps:   make(map[mapScanTypeKey]reflect.Type),
		keys:   make(map[reflect.Type]bool),
	}
	registerScalarContainers[sql.NullBool](t)
	registerScalarContainers[sql.NullString](t)
	registerScalarContainers[[]byte](t)
	registerScalarContainers[sql.NullInt32](t)
	registerScalarContainers[sql.NullInt64](t)
	registerScalarContainers[sql.NullFloat64](t)
	registerScalarContainers[sql.NullTime](t)
	registerScalarContainers[interface{}](t)
	registerScalarContainers[Row](t)
	registerScalarContainers[Variant](t)
	return t
}

// registerScalarContainers registers the containers of the scalar scan type
// S, which must cover every type scalarScanType returns.
func registerScalarContainers[S any](t *scanTypeTable) {
	registerArrays[S](t)
	registerMapsWithKey[sql.NullBool, S](t)
	registerMapsWithKey[sql.NullString, S](t)
	registerMapsWithKey[sql.NullInt32, S](t)
	registerMapsWithKey[sql.NullInt64, S](t)
	registerMapsWithKey[sql.NullFloat64, S](t)
	registerMapsWithKey[sql.NullTime, S](t)
	registerMapsWithKey[interface{}, S](t)
}

func registerMapsWithKey[K comparable, S any](t *scanTypeTable) {
	registerMap[K, S](t)
	registerSlice[NullMap[K, S]](t)
}

// registerArrays registers arrays of up to three dimensions of E.
func registerArrays[E any](t *scanTypeTable) {
	registerSlice[E](t)
	registerSlice[NullSlice[E]](t)
	registerSlice[NullSlice[NullSlice[E]]](t)
}

func registerSlice[E any](t *scanTypeTable) {
	t.slices[reflect.TypeFor[E]()] = reflect.TypeFor[NullSlice[E]]()
}

func registerMap[K comparable, V any](t *scanTypeTable) {
	t.keys[reflect.TypeFor[K]()] = true
	t.maps[mapScanTypeKey{reflect.TypeFor[K](), reflect.TypeFor[V]()}] = reflect.TypeFor[NullMap[K, V]]()
}

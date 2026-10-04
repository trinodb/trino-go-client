package trino

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Variant represents a VARIANT value, in the binary encoding defined by the
// Parquet variant specification, which Iceberg uses too. Valid is false for
// SQL NULL. A VARIANT null, like CAST(JSON 'null' AS VARIANT), is a valid
// Variant whose Type is VariantNull.
type Variant struct {
	metadata variantMetadata
	value    []byte
	// location interprets the values that have no time zone: dates, times and
	// timestamps without a time zone.
	location *time.Location
	Valid    bool
}

// VariantType is the type of the value a Variant holds. The names follow the
// Java client's Variant.ValueType.
type VariantType int

const (
	VariantNull VariantType = iota
	VariantBoolean
	VariantInt8
	VariantInt16
	VariantInt32
	VariantInt64
	VariantDouble
	VariantDecimal
	VariantDate
	VariantTimestampUTCMicros
	VariantTimestampNTZMicros
	VariantFloat
	VariantBinary
	VariantString
	VariantTimeNTZMicros
	VariantTimestampUTCNanos
	VariantTimestampNTZNanos
	VariantUUID
	VariantObject
	VariantArray
)

var variantTypeNames = [...]string{
	VariantNull:               "NULL",
	VariantBoolean:            "BOOLEAN",
	VariantInt8:               "INT8",
	VariantInt16:              "INT16",
	VariantInt32:              "INT32",
	VariantInt64:              "INT64",
	VariantDouble:             "DOUBLE",
	VariantDecimal:            "DECIMAL",
	VariantDate:               "DATE",
	VariantTimestampUTCMicros: "TIMESTAMP_UTC_MICROS",
	VariantTimestampNTZMicros: "TIMESTAMP_NTZ_MICROS",
	VariantFloat:              "FLOAT",
	VariantBinary:             "BINARY",
	VariantString:             "STRING",
	VariantTimeNTZMicros:      "TIME_NTZ_MICROS",
	VariantTimestampUTCNanos:  "TIMESTAMP_UTC_NANOS",
	VariantTimestampNTZNanos:  "TIMESTAMP_NTZ_NANOS",
	VariantUUID:               "UUID",
	VariantObject:             "OBJECT",
	VariantArray:              "ARRAY",
}

func (t VariantType) String() string {
	if t < 0 || int(t) >= len(variantTypeNames) {
		return "VariantType(" + strconv.Itoa(int(t)) + ")"
	}
	return variantTypeNames[t]
}

// Type returns the type of the value. It returns VariantNull for SQL NULL.
func (v Variant) Type() VariantType {
	if !v.Valid {
		return VariantNull
	}
	switch variantBasicType(v.value[0]) {
	case variantBasicShortString:
		return VariantString
	case variantBasicObject:
		return VariantObject
	case variantBasicArray:
		return VariantArray
	}
	return variantPrimitiveTypes[variantPrimitiveID(v.value[0])].valueType
}

// Value returns the value as a Go value, converting objects and arrays
// recursively:
//   - VariantNull: nil, also for SQL NULL
//   - VariantBoolean: bool
//   - VariantInt8, VariantInt16, VariantInt32 and VariantInt64: int64
//   - VariantFloat: float32
//   - VariantDouble: float64
//   - VariantDecimal: Numeric, in plain notation like "-12.345"
//   - VariantString: string
//   - VariantBinary: []byte
//   - VariantUUID: string, like "12151fd2-7586-11e9-8f9e-2a86e4085a59"
//   - VariantDate: time.Time at midnight
//   - VariantTimeNTZMicros: time.Time on January 1st of year 0, like a TIME
//     column
//   - VariantTimestampUTCMicros and VariantTimestampUTCNanos: time.Time in UTC
//   - VariantTimestampNTZMicros and VariantTimestampNTZNanos: time.Time
//   - VariantArray: []interface{}
//   - VariantObject: map[string]interface{}
//
// Dates, times and timestamps without a time zone are in the location the
// driver interprets TIMESTAMP columns in.
func (v Variant) Value() interface{} {
	if !v.Valid {
		return nil
	}
	return v.goValue()
}

// String returns the value as JSON text, the same text the server returns
// for CAST(v AS JSON), like {"a":1,"b":[true,null]}. Values JSON has no type
// for are written as strings, like "2020-01-02" for a date, a base64 string
// for binary values and "NaN" for a NaN. Both a VARIANT null and SQL NULL
// are written as null.
func (v Variant) String() string {
	return string(v.appendJSON(nil))
}

// MarshalJSON implements the json.Marshaler interface, encoding the value
// as String does.
func (v Variant) MarshalJSON() ([]byte, error) {
	return v.appendJSON(nil), nil
}

// Scan implements the sql.Scanner interface.
func (v *Variant) Scan(value interface{}) error {
	if value == nil {
		*v = Variant{}
		return nil
	}
	vv, ok := value.(Variant)
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to Variant", value, value)
	}
	*v = vv
	return nil
}

// variantMaxDepth bounds how deeply arrays and objects can nest, so that a
// malformed value cannot exhaust the stack. It is the limit encoding/json
// applies.
const variantMaxDepth = 10000

// decodeVariant converts the object the server sends for a VARIANT value
// once the VARIANT_BINARY capability is announced: the base64-encoded
// metadata and value of the binary encoding.
func decodeVariant(v interface{}, location *time.Location) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	encoded, ok := v.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot convert %v (%T) to variant", v, v)
	}
	metadata, err := decodeVariantField(encoded, "metadata")
	if err != nil {
		return nil, err
	}
	value, err := decodeVariantField(encoded, "value")
	if err != nil {
		return nil, err
	}
	return newVariant(metadata, value, location)
}

func decodeVariantField(encoded map[string]interface{}, name string) ([]byte, error) {
	text, ok := encoded[name].(string)
	if !ok {
		return nil, fmt.Errorf("variant has no %s", name)
	}
	decoded, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return nil, fmt.Errorf("variant %s is not base64: %w", name, err)
	}
	return decoded, nil
}

// newVariant validates the whole value up front, so that reading it later,
// which slices the bytes without checking, cannot panic.
func newVariant(metadataBytes, value []byte, location *time.Location) (Variant, error) {
	metadata, err := parseVariantMetadata(metadataBytes)
	if err != nil {
		return Variant{}, err
	}
	size, err := validateVariantValue(metadata, value, 0)
	if err != nil {
		return Variant{}, err
	}
	if size != len(value) {
		return Variant{}, fmt.Errorf("variant value has %d bytes but encodes %d", len(value), size)
	}
	if location == nil {
		location = time.UTC
	}
	return Variant{metadata: metadata, value: value, location: location, Valid: true}, nil
}

func (v Variant) goValue() interface{} {
	switch v.Type() {
	case VariantNull:
		return nil
	case VariantBoolean:
		return variantPrimitiveID(v.value[0]) == variantPrimitiveTrue
	case VariantInt8:
		return int64(int8(v.value[1]))
	case VariantInt16:
		return int64(int16(binary.LittleEndian.Uint16(v.value[1:])))
	case VariantInt32:
		return int64(v.int32())
	case VariantInt64:
		return v.int64()
	case VariantFloat:
		return math.Float32frombits(binary.LittleEndian.Uint32(v.value[1:]))
	case VariantDouble:
		return math.Float64frombits(binary.LittleEndian.Uint64(v.value[1:]))
	case VariantDecimal:
		unscaled, scale := v.decimal()
		return Numeric(plainDecimal(unscaled, scale))
	case VariantString:
		return v.string()
	case VariantBinary:
		return append([]byte{}, v.binary()...)
	case VariantUUID:
		return v.uuid()
	case VariantDate:
		return time.Date(1970, time.January, 1+int(v.int32()), 0, 0, 0, 0, v.location)
	case VariantTimeNTZMicros:
		return time.Date(0, time.January, 1, 0, 0, 0, 0, v.location).Add(time.Duration(v.int64()) * time.Microsecond)
	case VariantTimestampUTCMicros:
		return time.UnixMicro(v.int64()).UTC()
	case VariantTimestampUTCNanos:
		return time.Unix(0, v.int64()).UTC()
	case VariantTimestampNTZMicros:
		return inLocation(time.UnixMicro(v.int64()).UTC(), v.location)
	case VariantTimestampNTZNanos:
		return inLocation(time.Unix(0, v.int64()).UTC(), v.location)
	case VariantArray:
		elements := make([]interface{}, v.arrayLength())
		for i := range elements {
			elements[i] = v.arrayElement(i).goValue()
		}
		return elements
	case VariantObject:
		count := v.objectLength()
		fields := make(map[string]interface{}, count)
		for i := 0; i < count; i++ {
			name, value := v.objectField(i)
			fields[name] = value.goValue()
		}
		return fields
	}
	panic("unhandled variant type " + v.Type().String())
}

// appendJSON writes the value the way the server's VariantUtil.asJson and the
// Java client's Variant.toJson do, so the text matches what a VARIANT column
// returned before the driver announced VARIANT_BINARY.
func (v Variant) appendJSON(buf []byte) []byte {
	if !v.Valid {
		return append(buf, "null"...)
	}
	switch v.Type() {
	case VariantNull:
		return append(buf, "null"...)
	case VariantBoolean, VariantInt8, VariantInt16, VariantInt32, VariantInt64:
		return fmt.Append(buf, v.goValue())
	case VariantFloat:
		return appendJavaFloat(buf, float64(v.goValue().(float32)), 32)
	case VariantDouble:
		return appendJavaFloat(buf, v.goValue().(float64), 64)
	case VariantDecimal:
		unscaled, scale := v.decimal()
		return append(buf, javaDecimal(unscaled, scale)...)
	case VariantString:
		return appendJSONString(buf, v.string())
	case VariantBinary:
		return appendJSONString(buf, base64.StdEncoding.EncodeToString(v.binary()))
	case VariantUUID:
		return appendJSONString(buf, v.uuid())
	case VariantDate:
		return appendJSONString(buf, time.Unix(int64(v.int32())*86400, 0).UTC().Format("2006-01-02"))
	case VariantTimeNTZMicros:
		return appendJSONString(buf, time.UnixMicro(v.int64()).UTC().Format("15:04:05.000000"))
	case VariantTimestampUTCMicros:
		return appendJSONString(buf, time.UnixMicro(v.int64()).UTC().Format("2006-01-02 15:04:05.000000 UTC"))
	case VariantTimestampNTZMicros:
		return appendJSONString(buf, time.UnixMicro(v.int64()).UTC().Format("2006-01-02 15:04:05.000000"))
	case VariantTimestampUTCNanos:
		return appendJSONString(buf, time.Unix(0, v.int64()).UTC().Format("2006-01-02 15:04:05.000000000 UTC"))
	case VariantTimestampNTZNanos:
		return appendJSONString(buf, time.Unix(0, v.int64()).UTC().Format("2006-01-02 15:04:05.000000000"))
	case VariantArray:
		buf = append(buf, '[')
		for i := 0; i < v.arrayLength(); i++ {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = v.arrayElement(i).appendJSON(buf)
		}
		return append(buf, ']')
	case VariantObject:
		buf = append(buf, '{')
		for i := 0; i < v.objectLength(); i++ {
			if i > 0 {
				buf = append(buf, ',')
			}
			name, value := v.objectField(i)
			buf = appendJSONString(buf, name)
			buf = append(buf, ':')
			buf = value.appendJSON(buf)
		}
		return append(buf, '}')
	}
	panic("unhandled variant type " + v.Type().String())
}

func inLocation(utc time.Time, location *time.Location) time.Time {
	return time.Date(utc.Year(), utc.Month(), utc.Day(), utc.Hour(), utc.Minute(), utc.Second(), utc.Nanosecond(), location)
}

func (v Variant) int32() int32 {
	return int32(binary.LittleEndian.Uint32(v.value[1:]))
}

func (v Variant) int64() int64 {
	return int64(binary.LittleEndian.Uint64(v.value[1:]))
}

func (v Variant) decimal() (*big.Int, int) {
	scale := int(v.value[1])
	switch variantPrimitiveID(v.value[0]) {
	case variantPrimitiveDecimal4:
		return big.NewInt(int64(int32(binary.LittleEndian.Uint32(v.value[2:])))), scale
	case variantPrimitiveDecimal8:
		return big.NewInt(int64(binary.LittleEndian.Uint64(v.value[2:]))), scale
	}
	bigEndian := make([]byte, 16)
	for i := range bigEndian {
		bigEndian[i] = v.value[2+15-i]
	}
	unscaled := new(big.Int).SetBytes(bigEndian)
	if bigEndian[0]&0x80 != 0 {
		unscaled.Sub(unscaled, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	return unscaled, scale
}

func (v Variant) string() string {
	if variantBasicType(v.value[0]) == variantBasicShortString {
		return string(v.value[1 : 1+int(v.value[0]>>2)])
	}
	return string(v.binary())
}

func (v Variant) binary() []byte {
	length := binary.LittleEndian.Uint32(v.value[1:])
	return v.value[5 : 5+length]
}

func (v Variant) uuid() string {
	text := hex.EncodeToString(v.value[1:17])
	return text[:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:]
}

func (v Variant) arrayLength() int {
	return newVariantContainer(v.value).count
}

func (v Variant) arrayElement(i int) Variant {
	container := newVariantContainer(v.value)
	start := container.offset(i)
	end := container.offset(i + 1)
	return v.child(v.value[container.valuesStart+start : container.valuesStart+end])
}

func (v Variant) objectLength() int {
	return newVariantContainer(v.value).count
}

// objectField returns the i-th field in the order the encoding stores them,
// which the specification requires to be sorted by field name.
func (v Variant) objectField(i int) (string, Variant) {
	container := newVariantContainer(v.value)
	fieldID := int(readVariantUint(v.value[container.idsStart+i*container.idSize:], container.idSize))
	start := container.valuesStart + container.offset(i)
	// Object field offsets need not be sorted, so a field's size comes from
	// its own header rather than from the next offset.
	size, _ := variantValueSize(v.value[start:])
	return v.metadata.key(fieldID), v.child(v.value[start : start+size])
}

func (v Variant) child(value []byte) Variant {
	return Variant{metadata: v.metadata, value: value, location: v.location, Valid: true}
}

const (
	variantBasicPrimitive = iota
	variantBasicShortString
	variantBasicObject
	variantBasicArray
)

const (
	variantPrimitiveNull = iota
	variantPrimitiveTrue
	variantPrimitiveFalse
	variantPrimitiveInt8
	variantPrimitiveInt16
	variantPrimitiveInt32
	variantPrimitiveInt64
	variantPrimitiveDouble
	variantPrimitiveDecimal4
	variantPrimitiveDecimal8
	variantPrimitiveDecimal16
	variantPrimitiveDate
	variantPrimitiveTimestampUTCMicros
	variantPrimitiveTimestampNTZMicros
	variantPrimitiveFloat
	variantPrimitiveBinary
	variantPrimitiveString
	variantPrimitiveTimeNTZMicros
	variantPrimitiveTimestampUTCNanos
	variantPrimitiveTimestampNTZNanos
	variantPrimitiveUUID
)

// variantPrimitiveTypes is indexed by the primitive type ID in a value
// header. A size of 0 means the value is a 4-byte length followed by that
// many bytes.
var variantPrimitiveTypes = [...]struct {
	valueType VariantType
	size      int
}{
	variantPrimitiveNull:               {VariantNull, 1},
	variantPrimitiveTrue:               {VariantBoolean, 1},
	variantPrimitiveFalse:              {VariantBoolean, 1},
	variantPrimitiveInt8:               {VariantInt8, 2},
	variantPrimitiveInt16:              {VariantInt16, 3},
	variantPrimitiveInt32:              {VariantInt32, 5},
	variantPrimitiveInt64:              {VariantInt64, 9},
	variantPrimitiveDouble:             {VariantDouble, 9},
	variantPrimitiveDecimal4:           {VariantDecimal, 6},
	variantPrimitiveDecimal8:           {VariantDecimal, 10},
	variantPrimitiveDecimal16:          {VariantDecimal, 18},
	variantPrimitiveDate:               {VariantDate, 5},
	variantPrimitiveTimestampUTCMicros: {VariantTimestampUTCMicros, 9},
	variantPrimitiveTimestampNTZMicros: {VariantTimestampNTZMicros, 9},
	variantPrimitiveFloat:              {VariantFloat, 5},
	variantPrimitiveBinary:             {VariantBinary, 0},
	variantPrimitiveString:             {VariantString, 0},
	variantPrimitiveTimeNTZMicros:      {VariantTimeNTZMicros, 9},
	variantPrimitiveTimestampUTCNanos:  {VariantTimestampUTCNanos, 9},
	variantPrimitiveTimestampNTZNanos:  {VariantTimestampNTZNanos, 9},
	variantPrimitiveUUID:               {VariantUUID, 17},
}

func variantBasicType(header byte) int {
	return int(header & 0b11)
}

func variantPrimitiveID(header byte) int {
	return int(header >> 2)
}

// variantContainer is the layout of an object or array header: the element
// count, then for an object the field IDs, then count+1 offsets relative to
// valuesStart, where the last offset is the size of all the values.
type variantContainer struct {
	value       []byte
	count       int
	idSize      int
	idsStart    int
	offsetSize  int
	offsetStart int
	valuesStart int
}

func newVariantContainer(value []byte) variantContainer {
	header := value[0] >> 2
	container := variantContainer{value: value, offsetSize: int(header&0b11) + 1}
	if variantBasicType(value[0]) == variantBasicObject {
		container.idSize = int(header>>2&0b11) + 1
	}
	countSize := variantContainerCountSize(value[0])
	container.count = int(readVariantUint(value[1:], countSize))
	container.idsStart = 1 + countSize
	container.offsetStart = container.idsStart + container.count*container.idSize
	container.valuesStart = container.offsetStart + (container.count+1)*container.offsetSize
	return container
}

func (c variantContainer) offset(i int) int {
	return int(readVariantUint(c.value[c.offsetStart+i*c.offsetSize:], c.offsetSize))
}

// variantContainerCountSize returns how many bytes hold the element count of
// an object or array: 4 when its header has the is_large bit, otherwise 1.
func variantContainerCountSize(header byte) int {
	largeBit := byte(0b100)
	if variantBasicType(header) == variantBasicObject {
		largeBit = 0b1_0000
	}
	if header>>2&largeBit != 0 {
		return 4
	}
	return 1
}

// variantValueSize returns how many bytes the value starting at value[0]
// takes, checking only that its header and, for an object or array, its
// element layout fit in value.
func variantValueSize(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, errors.New("variant value is empty")
	}
	switch variantBasicType(value[0]) {
	case variantBasicPrimitive:
		id := variantPrimitiveID(value[0])
		if id >= len(variantPrimitiveTypes) {
			return 0, fmt.Errorf("unknown variant primitive type %d", id)
		}
		size := variantPrimitiveTypes[id].size
		if size != 0 {
			return size, nil
		}
		if len(value) < 5 {
			return 0, errors.New("variant value is truncated")
		}
		return 5 + int(binary.LittleEndian.Uint32(value[1:])), nil
	case variantBasicShortString:
		return 1 + int(value[0]>>2), nil
	}
	countSize := variantContainerCountSize(value[0])
	if len(value) < 1+countSize {
		return 0, errors.New("variant value is truncated")
	}
	// Every element takes at least one offset byte, so a larger count is
	// malformed; checking it first also keeps the layout arithmetic below
	// from overflowing.
	if count := readVariantUint(value[1:], countSize); uint64(count) > uint64(len(value)) {
		return 0, errors.New("variant value is truncated")
	}
	container := newVariantContainer(value)
	if container.valuesStart > len(value) {
		return 0, errors.New("variant value is truncated")
	}
	return container.valuesStart + container.offset(container.count), nil
}

// validateVariantValue checks that value starts with a well-formed variant
// value, including all nested values, and returns its size.
func validateVariantValue(metadata variantMetadata, value []byte, depth int) (int, error) {
	size, err := variantValueSize(value)
	if err != nil {
		return 0, err
	}
	if size > len(value) {
		return 0, errors.New("variant value is truncated")
	}
	value = value[:size]
	basicType := variantBasicType(value[0])
	if basicType == variantBasicPrimitive || basicType == variantBasicShortString {
		return size, nil
	}
	if depth >= variantMaxDepth {
		return 0, fmt.Errorf("variant value nests deeper than %d levels", variantMaxDepth)
	}
	container := newVariantContainer(value)
	valuesSize := size - container.valuesStart
	for i := 0; i < container.count; i++ {
		start := container.offset(i)
		end := valuesSize
		if basicType == variantBasicArray {
			end = container.offset(i + 1)
		} else if fieldID := int(readVariantUint(value[container.idsStart+i*container.idSize:], container.idSize)); fieldID >= metadata.size {
			return 0, fmt.Errorf("variant field ID %d is not in the metadata dictionary of %d keys", fieldID, metadata.size)
		}
		if start > end || end > valuesSize {
			return 0, fmt.Errorf("variant element %d has invalid offsets %d and %d", i, start, end)
		}
		if _, err := validateVariantValue(metadata, value[container.valuesStart+start:container.valuesStart+end], depth+1); err != nil {
			return 0, err
		}
	}
	return size, nil
}

// variantMetadata is the dictionary of object field names a value refers to
// by ID: a header, the dictionary size, size+1 offsets, then the names.
type variantMetadata struct {
	bytes       []byte
	offsetSize  int
	size        int
	stringStart int
}

func parseVariantMetadata(metadata []byte) (variantMetadata, error) {
	// The Java client accepts empty metadata as an empty dictionary.
	if len(metadata) == 0 {
		return variantMetadata{offsetSize: 1}, nil
	}
	if version := metadata[0] & 0b1111; version != 1 {
		return variantMetadata{}, fmt.Errorf("unsupported variant metadata version %d", version)
	}
	result := variantMetadata{bytes: metadata, offsetSize: int(metadata[0]>>6) + 1}
	if len(metadata) < 1+result.offsetSize {
		return variantMetadata{}, errors.New("variant metadata is truncated")
	}
	if size := readVariantUint(metadata[1:], result.offsetSize); uint64(size) > uint64(len(metadata)) {
		return variantMetadata{}, errors.New("variant metadata is truncated")
	}
	result.size = int(readVariantUint(metadata[1:], result.offsetSize))
	result.stringStart = 1 + (result.size+2)*result.offsetSize
	if result.stringStart > len(metadata) {
		return variantMetadata{}, errors.New("variant metadata is truncated")
	}
	previous := 0
	for id := 0; id <= result.size; id++ {
		offset := result.offset(id)
		if offset < previous || result.stringStart+offset > len(metadata) {
			return variantMetadata{}, fmt.Errorf("variant metadata has an invalid offset for key %d", id)
		}
		previous = offset
	}
	return result, nil
}

func (m variantMetadata) offset(id int) int {
	return int(readVariantUint(m.bytes[1+(id+1)*m.offsetSize:], m.offsetSize))
}

func (m variantMetadata) key(id int) string {
	return string(m.bytes[m.stringStart+m.offset(id) : m.stringStart+m.offset(id+1)])
}

// readVariantUint reads a little-endian unsigned integer of 1 to 4 bytes.
func readVariantUint(b []byte, size int) uint32 {
	var result uint32
	for i := size - 1; i >= 0; i-- {
		result = result<<8 | uint32(b[i])
	}
	return result
}

// appendJavaFloat formats a float the way Java's Double.toString and
// Float.toString do, which the server uses for VARIANT JSON: the shortest
// digits that round-trip, in plain notation from 10^-3 up to 10^7 and in
// scientific notation like 1.0E10 otherwise. JSON has no NaN or infinity, so
// those are written as strings.
func appendJavaFloat(buf []byte, f float64, bitSize int) []byte {
	switch {
	case math.IsNaN(f):
		return append(buf, `"NaN"`...)
	case math.IsInf(f, 1):
		return append(buf, `"Infinity"`...)
	case math.IsInf(f, -1):
		return append(buf, `"-Infinity"`...)
	}
	if abs := math.Abs(f); abs == 0 || abs >= 1e-3 && abs < 1e7 {
		text := strconv.FormatFloat(f, 'f', -1, bitSize)
		if !strings.Contains(text, ".") {
			text += ".0"
		}
		return append(buf, text...)
	}
	mantissa, exponent, _ := strings.Cut(strconv.FormatFloat(f, 'e', -1, bitSize), "e")
	if !strings.Contains(mantissa, ".") {
		mantissa += ".0"
	}
	exponentValue, _ := strconv.Atoi(exponent)
	buf = append(buf, mantissa...)
	buf = append(buf, 'E')
	return strconv.AppendInt(buf, int64(exponentValue), 10)
}

// javaDecimal formats a decimal the way Java's BigDecimal.toString does,
// which the server uses for VARIANT JSON: in scientific notation like 1E-7
// when the exponent of its first digit is below -6. The scale of a variant
// decimal is never negative, which is the other case BigDecimal.toString
// uses scientific notation for.
func javaDecimal(unscaled *big.Int, scale int) string {
	digits := new(big.Int).Abs(unscaled).String()
	sign := ""
	if unscaled.Sign() < 0 {
		sign = "-"
	}
	adjustedExponent := len(digits) - 1 - scale
	if adjustedExponent >= -6 {
		return plainDecimal(unscaled, scale)
	}
	mantissa := digits[:1]
	if len(digits) > 1 {
		mantissa += "." + digits[1:]
	}
	return sign + mantissa + "E" + strconv.Itoa(adjustedExponent)
}

func plainDecimal(unscaled *big.Int, scale int) string {
	digits := new(big.Int).Abs(unscaled).String()
	sign := ""
	if unscaled.Sign() < 0 {
		sign = "-"
	}
	if scale == 0 {
		return sign + digits
	}
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	return sign + digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
}

// appendJSONString quotes s the way Jackson does: only quotes, backslashes
// and control characters are escaped, so unlike encoding/json it leaves
// HTML characters, U+2028 and U+2029 as they are.
func appendJSONString(buf []byte, s string) []byte {
	const hexDigits = "0123456789ABCDEF"
	buf = append(buf, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\\':
			buf = append(buf, '\\', c)
		case c >= 0x20:
			buf = append(buf, c)
		case c == '\b':
			buf = append(buf, `\b`...)
		case c == '\t':
			buf = append(buf, `\t`...)
		case c == '\n':
			buf = append(buf, `\n`...)
		case c == '\f':
			buf = append(buf, `\f`...)
		case c == '\r':
			buf = append(buf, `\r`...)
		default:
			buf = append(buf, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
		}
	}
	return append(buf, '"')
}

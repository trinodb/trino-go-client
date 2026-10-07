package trino

import (
	"bytes"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// rowsDecoder decodes the rows of a page or a segment, an array holding one
// array of values per row, straight into the values Next returns.
type rowsDecoder struct {
	columns  []string
	decoders []valueDecoder
}

// newRowsDecoder builds the decoders of the columns once, so decoding a
// value only follows the decoder tree of its type. Values without a time
// zone are interpreted in location.
func newRowsDecoder(columns []queryColumn, location *time.Location) (*rowsDecoder, error) {
	result := &rowsDecoder{
		columns:  make([]string, len(columns)),
		decoders: make([]valueDecoder, len(columns)),
	}
	for i, column := range columns {
		decoder, err := newValueDecoder(column.TypeSignature, location)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", column.Name, err)
		}
		result.columns[i] = column.Name
		result.decoders[i] = decoder
	}
	return result, nil
}

// decodeRows decodes the rows in data. expectedRows only sizes the result:
// the values of that many rows share one allocation, and any rows beyond it
// get their own.
func (d *rowsDecoder) decodeRows(data []byte, expectedRows int) ([]queryData, error) {
	// Reading from a bytes.Buffer lets the decoder parse data in place.
	dec := jsontext.NewDecoder(bytes.NewBuffer(data))
	if err := readDelimiter(dec, '[', "an array of rows"); err != nil {
		return nil, err
	}
	rows := make([]queryData, 0, expectedRows)
	values := d.valuesOfRows(expectedRows, len(data))
	columns := len(d.decoders)
	for dec.PeekKind() != ']' {
		var row queryData
		if len(values) > 0 {
			// The full slice expression stops an append to the row from
			// overwriting the next one.
			row, values = values[:columns:columns], values[columns:]
		} else {
			row = make(queryData, columns)
		}
		if err := d.decodeRow(dec, row); err != nil {
			return nil, fmt.Errorf("row %d: %w", len(rows), err)
		}
		rows = append(rows, row)
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return nil, fmt.Errorf("unexpected data after the rows: %v", err)
	}
	return rows, nil
}

// valuesOfRows allocates the values of expectedRows rows at once. Every value
// takes at least one byte of data, so a row count that data cannot hold is
// not trusted with an allocation of its size.
func (d *rowsDecoder) valuesOfRows(expectedRows int, dataSize int) []driver.Value {
	values := expectedRows * len(d.decoders)
	if expectedRows <= 0 || values > dataSize {
		return nil
	}
	return make([]driver.Value, values)
}

// decodeRow decodes the values of a row into row, which has one element per
// column.
func (d *rowsDecoder) decodeRow(dec *jsontext.Decoder, row queryData) error {
	if err := readDelimiter(dec, '[', "an array of column values"); err != nil {
		return err
	}
	for i, decoder := range d.decoders {
		if dec.PeekKind() == ']' {
			return fmt.Errorf("no value for column %q", d.columns[i])
		}
		value, err := decodeValue(dec, decoder)
		if err != nil {
			return fmt.Errorf("column %q: %w", d.columns[i], err)
		}
		row[i] = value
	}
	token, err := dec.ReadToken()
	if err != nil {
		return err
	}
	if token.Kind() != ']' {
		return fmt.Errorf("more values than the %d columns", len(d.decoders))
	}
	return nil
}

// valueDecoder decodes a value of one type, other than NULL, into the Go
// value the driver returns for that type.
type valueDecoder interface {
	decode(dec *jsontext.Decoder) (any, error)
}

// decodeValue decodes a value of any type, which can be NULL.
func decodeValue(dec *jsontext.Decoder, decoder valueDecoder) (any, error) {
	if dec.PeekKind() == 'n' {
		_, err := dec.ReadToken()
		return nil, err
	}
	return decoder.decode(dec)
}

// newValueDecoder returns the decoder of the type signature describes.
// Values without a time zone are interpreted in location.
func newValueDecoder(signature typeSignature, location *time.Location) (valueDecoder, error) {
	switch signature.RawType {
	case "boolean":
		return booleanDecoder{}, nil
	case "tinyint", "smallint", "integer", "bigint":
		return integerDecoder{}, nil
	case "real", "double":
		return floatDecoder{}, nil
	case "json", "char", "varchar", "interval year to month", "interval day to second", "decimal", "number", "ipaddress", "uuid", "Geometry", "SphericalGeography", "color", "unknown":
		return stringDecoder{}, nil
	case "date", "time", "time with time zone", "timestamp", "timestamp with time zone":
		return timeDecoder{location: location}, nil
	case "variant":
		return variantDecoder{location: location}, nil
	case "KdbTree", "BingTile":
		return jsonDecoder{}, nil
	case "array":
		if len(signature.Arguments) != 1 {
			return nil, ErrInvalidResponseType
		}
		element, err := argumentDecoder(signature.Arguments[0], location)
		if err != nil {
			return nil, err
		}
		return arrayDecoder{element: element}, nil
	case "map":
		if len(signature.Arguments) != 2 {
			return nil, ErrInvalidResponseType
		}
		// The JSON response sends the keys as object member names, so only
		// the values need a decoder.
		value, err := argumentDecoder(signature.Arguments[1], location)
		if err != nil {
			return nil, err
		}
		return mapDecoder{value: value}, nil
	case "row":
		return newRowDecoder(signature, location)
	default:
		// every type without a textual form, like HyperLogLog or SetDigest, arrives as base64 like varbinary
		return binaryDecoder{}, nil
	}
}

func argumentDecoder(argument typeArgument, location *time.Location) (valueDecoder, error) {
	signature, err := typeArgumentSignature(argument)
	if err != nil {
		return nil, err
	}
	return newValueDecoder(signature, location)
}

func newRowDecoder(signature typeSignature, location *time.Location) (valueDecoder, error) {
	result := rowDecoder{
		names:  make([]string, len(signature.Arguments)),
		fields: make([]valueDecoder, len(signature.Arguments)),
	}
	for i, argument := range signature.Arguments {
		result.names[i] = argument.namedTypeSignature.FieldName.Name
		if result.names[i] == "" {
			result.names[i] = "field" + strconv.Itoa(i)
		}
		field, err := argumentDecoder(argument, location)
		if err != nil {
			return nil, err
		}
		result.fields[i] = field
	}
	return result, nil
}

type booleanDecoder struct{}

func (booleanDecoder) decode(dec *jsontext.Decoder) (any, error) {
	token, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if kind := token.Kind(); kind != 't' && kind != 'f' {
		return nil, unexpectedKind(kind, "a boolean")
	}
	return token.Bool(), nil
}

type integerDecoder struct{}

func (integerDecoder) decode(dec *jsontext.Decoder) (any, error) {
	token, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if kind := token.Kind(); kind != '0' {
		return nil, unexpectedKind(kind, "an integer")
	}
	value, err := token.Int()
	if errors.Is(err, strconv.ErrRange) {
		return nil, fmt.Errorf("integer %s is out of range", token.String())
	}
	if err != nil {
		return nil, fmt.Errorf("expected an integer, got %s", token.String())
	}
	return value, nil
}

type floatDecoder struct{}

func (floatDecoder) decode(dec *jsontext.Decoder) (any, error) {
	token, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	var value float64
	switch kind := token.Kind(); kind {
	case '0':
		value, err = token.Float()
	case '"':
		// JSON has no numbers for NaN and the infinities, which the server
		// sends as the strings "NaN", "Infinity" and "-Infinity".
		value, err = strconv.ParseFloat(token.String(), 64)
	default:
		return nil, unexpectedKind(kind, "a number")
	}
	if err != nil {
		return nil, err
	}
	return value, nil
}

type stringDecoder struct{}

func (stringDecoder) decode(dec *jsontext.Decoder) (any, error) {
	return readString(dec, "a string")
}

// binaryDecoder decodes the base64 form VARBINARY values, and those of
// every type without a textual form, travel in.
type binaryDecoder struct{}

func (binaryDecoder) decode(dec *jsontext.Decoder) (any, error) {
	return readBase64(dec)
}

type timeDecoder struct {
	location *time.Location
}

func (d timeDecoder) decode(dec *jsontext.Decoder) (any, error) {
	value, err := readString(dec, "a date or time string")
	if err != nil {
		return nil, err
	}
	return parseTime(value, d.location)
}

// variantDecoder decodes the object the server sends for a VARIANT value
// once the VARIANT_BINARY capability is announced: the base64-encoded
// metadata and value of the binary encoding.
type variantDecoder struct {
	location *time.Location
}

func (d variantDecoder) decode(dec *jsontext.Decoder) (any, error) {
	if err := readDelimiter(dec, '{', "a variant object"); err != nil {
		return nil, err
	}
	var metadata, value []byte
	for dec.PeekKind() != '}' {
		name, err := readString(dec, "a member name")
		if err != nil {
			return nil, err
		}
		switch name {
		case "metadata":
			metadata, err = readBase64(dec)
		case "value":
			value, err = readBase64(dec)
		default:
			err = dec.SkipValue()
		}
		if err != nil {
			return nil, fmt.Errorf("variant %s: %w", name, err)
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	if metadata == nil {
		return nil, errors.New("variant has no metadata")
	}
	if value == nil {
		return nil, errors.New("variant has no value")
	}
	return newVariant(metadata, value, d.location)
}

// jsonDecoder keeps a value in the shape of its JSON form, for the types
// the server sends as JSON objects, like BingTile and KdbTree: objects are
// map[string]interface{}, arrays []interface{} and numbers float64.
type jsonDecoder struct{}

func (jsonDecoder) decode(dec *jsontext.Decoder) (any, error) {
	var value any
	if err := json.UnmarshalDecode(dec, &value); err != nil {
		return nil, err
	}
	return value, nil
}

type arrayDecoder struct {
	element valueDecoder
}

func (d arrayDecoder) decode(dec *jsontext.Decoder) (any, error) {
	if err := readDelimiter(dec, '[', "an array"); err != nil {
		return nil, err
	}
	// An empty array stays non-nil, so it is not taken for a NULL. Room for
	// a few values up front saves the first reallocations of every array.
	values := make([]any, 0, 4)
	for dec.PeekKind() != ']' {
		value, err := decodeValue(dec, d.element)
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", len(values), err)
		}
		values = append(values, value)
	}
	_, err := dec.ReadToken()
	return values, err
}

type mapDecoder struct {
	value valueDecoder
}

func (d mapDecoder) decode(dec *jsontext.Decoder) (any, error) {
	if err := readDelimiter(dec, '{', "an object"); err != nil {
		return nil, err
	}
	values := make(map[string]any)
	for dec.PeekKind() != '}' {
		key, err := readString(dec, "a key")
		if err != nil {
			return nil, err
		}
		value, err := decodeValue(dec, d.value)
		if err != nil {
			return nil, fmt.Errorf("value for key %q: %w", key, err)
		}
		values[key] = value
	}
	_, err := dec.ReadToken()
	return values, err
}

// rowDecoder decodes a ROW, which the server sends as an array of its
// field values, into a Row. All the rows of a column share the names.
type rowDecoder struct {
	names  []string
	fields []valueDecoder
}

func (d rowDecoder) decode(dec *jsontext.Decoder) (any, error) {
	if err := readDelimiter(dec, '[', "an array of row fields"); err != nil {
		return nil, err
	}
	values := make([]any, len(d.fields))
	for i, field := range d.fields {
		if dec.PeekKind() == ']' {
			return nil, fmt.Errorf("row has %d fields but its type has %d", i, len(d.fields))
		}
		value, err := decodeValue(dec, field)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", d.names[i], err)
		}
		values[i] = value
	}
	extra := 0
	for dec.PeekKind() != ']' {
		if err := dec.SkipValue(); err != nil {
			return nil, err
		}
		extra++
	}
	if extra > 0 {
		return nil, fmt.Errorf("row has %d fields but its type has %d", len(d.fields)+extra, len(d.fields))
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	return Row{names: d.names, values: values, Valid: true}, nil
}

func readDelimiter(dec *jsontext.Decoder, delimiter jsontext.Kind, expected string) error {
	token, err := dec.ReadToken()
	if err != nil {
		return err
	}
	if kind := token.Kind(); kind != delimiter {
		return unexpectedKind(kind, expected)
	}
	return nil
}

func readString(dec *jsontext.Decoder, expected string) (string, error) {
	token, err := dec.ReadToken()
	if err != nil {
		return "", err
	}
	if kind := token.Kind(); kind != '"' {
		return "", unexpectedKind(kind, expected)
	}
	return token.String(), nil
}

func readBase64(dec *jsontext.Decoder) ([]byte, error) {
	encoded, err := readString(dec, "a base64 string")
	if err != nil {
		return nil, err
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("cannot decode base64 string: %w", err)
	}
	return decoded, nil
}

func unexpectedKind(kind jsontext.Kind, expected string) error {
	var got string
	switch kind {
	case 't', 'f':
		got = "a boolean"
	case '"':
		got = "a string"
	case '0':
		got = "a number"
	case '{':
		got = "an object"
	case '[':
		got = "an array"
	default:
		got = kind.String()
	}
	return fmt.Errorf("expected %s, got %s", expected, got)
}

// parseTime parses a Trino date, time or timestamp. Values that carry their
// own zone keep it; the others are interpreted in location.
func parseTime(value string, location *time.Location) (time.Time, error) {
	if i := strings.LastIndexByte(value, ' '); i >= 0 && i+1 < len(value) && !isDigit(value[i+1]) {
		return parseZonedTime(value)
	}
	// Time literals may not have spaces before the timezone.
	if i := strings.IndexByte(value, '+'); i >= 0 {
		return parseZonedTime(value[:i] + " " + value[i:])
	}
	hyphenCount := strings.Count(value, "-")
	// We need to ensure we don't treat the hyphens in dates as the minus offset sign.
	// So if there's only one hyphen or more than 2, we have a negative offset.
	if hyphenCount == 1 || hyphenCount > 2 {
		// We add a space before the last hyphen to parse properly.
		i := strings.LastIndexByte(value, '-')
		return parseZonedTime(value[:i] + " " + value[i:])
	}
	return time.ParseInLocation(localTimeLayout(value), value, location)
}

// parseZonedTime parses a time or timestamp followed by a space and either
// an offset, like +03:00, or a zone name, like Europe/Paris.
func parseZonedTime(value string) (time.Time, error) {
	i := strings.LastIndexByte(value, ' ')
	if i == -1 {
		return time.Time{}, fmt.Errorf("cannot convert %v (%T) to time+zone", value, value)
	}
	stamp, zone := value[:i], value[i+1:]
	if strings.HasPrefix(zone, "+") || strings.HasPrefix(zone, "-") {
		return time.Parse(localTimeLayout(stamp)+" -07:00", value)
	}
	location, err := loadZone(zone)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot load timezone %q: %v", zone, err)
	}
	return time.ParseInLocation(localTimeLayout(stamp), stamp, location)
}

// localTimeLayout returns the layout of a date, time or timestamp without
// a time zone, telling them apart by their separators.
// Trino can support up to 12 digits sub second precision, but Go only 9.
// (Requires X-Trino-Client-Capabilities: PARAMETRIC_DATETIME)
func localTimeLayout(value string) string {
	switch {
	case strings.IndexByte(value, ' ') >= 0:
		return "2006-01-02 15:04:05.999999999"
	case strings.IndexByte(value, ':') >= 0:
		return "15:04:05.999999999"
	default:
		return "2006-01-02"
	}
}

// zones caches the locations of zone names, as time.LoadLocation reads the
// zone database on every call and every TIMESTAMP WITH TIME ZONE value
// names its zone.
var zones sync.Map

func loadZone(name string) (*time.Location, error) {
	if location, ok := zones.Load(name); ok {
		return location.(*time.Location), nil
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	zones.Store(name, location)
	return location, nil
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

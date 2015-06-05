package pq

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq/oid"
)

func binaryEncode(parameterStatus *parameterStatus, x interface{}) []byte {
	switch v := x.(type) {
	case []byte:
		return v
	default:
		return encode(parameterStatus, x, oid.T_unknown)
	}
	panic("not reached")
}

func encode(parameterStatus *parameterStatus, x interface{}, pgtypOid oid.Oid) []byte {
	switch v := x.(type) {
	case int64:
		return strconv.AppendInt(nil, v, 10)
	case float64:
		return strconv.AppendFloat(nil, v, 'f', -1, 64)
	case []byte:
		if pgtypOid == oid.T_bytea {
			return encodeBytea(parameterStatus.serverVersion, v)
		}

		return v
	case string:
		if pgtypOid == oid.T_bytea {
			return encodeBytea(parameterStatus.serverVersion, []byte(v))
		}

		return []byte(v)
	case bool:
		return strconv.AppendBool(nil, v)
	case time.Time:
		return formatTs(v)

	default:
		errorf("encode: unknown type for %T", v)
	}

	panic("not reached")
}

func decode(parameterStatus *parameterStatus, s []byte, typ oid.Oid, f format) interface{} {
	if oid.IsArrayType(typ) {
		return arrayDecode(parameterStatus, s, typ, f)
	} else if f == formatBinary {
		return binaryDecode(parameterStatus, s, typ)
	} else {
		return textDecode(parameterStatus, s, typ)
	}
}

func binaryDecode(parameterStatus *parameterStatus, s []byte, typ oid.Oid) interface{} {
	switch typ {
	case oid.T_bytea:
		return s
	case oid.T_int8:
		return int64(binary.BigEndian.Uint64(s))
	case oid.T_int4:
		return int64(int32(binary.BigEndian.Uint32(s)))
	case oid.T_int2:
		return int64(int16(binary.BigEndian.Uint16(s)))

	default:
		errorf("don't know how to decode binary parameter of type %u", uint32(typ))
	}

	panic("not reached")
}

func textDecode(parameterStatus *parameterStatus, s []byte, typ oid.Oid) interface{} {
	switch typ {
	case oid.T_bytea:
		return parseBytea(s)
	case oid.T_timestamptz:
		return parseTs(parameterStatus.currentLocation, string(s))
	case oid.T_timestamp, oid.T_date:
		return parseTs(nil, string(s))
	case oid.T_time:
		return mustParse("15:04:05", typ, s)
	case oid.T_timetz:
		return mustParse("15:04:05-07", typ, s)
	case oid.T_bool:
		return s[0] == 't'
	case oid.T_int8, oid.T_int4, oid.T_int2:
		i, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			errorf("%s", err)
		}
		return i
	case oid.T_float4, oid.T_float8:
		bits := 64
		if typ == oid.T_float4 {
			bits = 32
		}
		f, err := strconv.ParseFloat(string(s), bits)
		if err != nil {
			errorf("%s", err)
		}
		return f
	}

	return s
}

type arrayValues struct {
	s     []byte
	Value []byte
	start int
	cur   int
	sep   byte
}

func (a *arrayValues) Next() bool {
	if a.cur == 0 {
		a.cur = 1
		a.start = 1
		a.sep = byte(int(','))
	}
	for a.cur < len(a.s)-1 {
		if a.s[a.cur] == a.sep {
			a.Value = a.s[a.start:a.cur]
			a.start = a.cur + 1
			a.cur++
			return true
		}
		a.cur++
	}
	return false
}

func arrayDecode(parameterStatus *parameterStatus, s []byte, typ oid.Oid, f format) interface{} {
	values := &arrayValues{}
	values.s = s

	arrtyp := oid.GetArrayType(typ)
	switch arrtyp {
	case oid.T_bytea:
		var ret [][]byte
		for values.Next() {
			v := decode(parameterStatus, values.Value, arrtyp, f)
			ret = append(ret, v.([]byte))
		}
		return ret
	case oid.T_time, oid.T_timetz:
		var ret []time.Time
		for values.Next() {
			v := decode(parameterStatus, values.Value, arrtyp, f)
			ret = append(ret, v.(time.Time))
		}
		return ret
	case oid.T_bool:
		var ret []bool
		for values.Next() {
			v := decode(parameterStatus, values.Value, arrtyp, f)
			ret = append(ret, v.(bool))
		}
		return ret
	case oid.T_int8, oid.T_int4, oid.T_int2:
		var ret []int64
		for values.Next() {
			v := decode(parameterStatus, values.Value, arrtyp, f)
			ret = append(ret, v.(int64))
		}
		return ret
	default:
		var ret []interface{}
		for values.Next() {
			v := decode(parameterStatus, values.Value, arrtyp, f)
			ret = append(ret, v)
		}
		return ret
	}
}

// appendEncodedText encodes item in text format as required by COPY
// and appends to buf
func appendEncodedText(parameterStatus *parameterStatus, buf []byte, x interface{}) []byte {
	switch v := x.(type) {
	case int64:
		return strconv.AppendInt(buf, v, 10)
	case float64:
		return strconv.AppendFloat(buf, v, 'f', -1, 64)
	case []byte:
		encodedBytea := encodeBytea(parameterStatus.serverVersion, v)
		return appendEscapedText(buf, string(encodedBytea))
	case string:
		return appendEscapedText(buf, v)
	case bool:
		return strconv.AppendBool(buf, v)
	case time.Time:
		return append(buf, formatTs(v)...)
	case nil:
		return append(buf, "\\N"...)
	default:
		errorf("encode: unknown type for %T", v)
	}

	panic("not reached")
}

func appendEscapedText(buf []byte, text string) []byte {
	escapeNeeded := false
	startPos := 0
	var c byte

	// check if we need to escape
	for i := 0; i < len(text); i++ {
		c = text[i]
		if c == '\\' || c == '\n' || c == '\r' || c == '\t' {
			escapeNeeded = true
			startPos = i
			break
		}
	}
	if !escapeNeeded {
		return append(buf, text...)
	}

	// copy till first char to escape, iterate the rest
	result := append(buf, text[:startPos]...)
	for i := startPos; i < len(text); i++ {
		c = text[i]
		switch c {
		case '\\':
			result = append(result, '\\', '\\')
		case '\n':
			result = append(result, '\\', 'n')
		case '\r':
			result = append(result, '\\', 'r')
		case '\t':
			result = append(result, '\\', 't')
		default:
			result = append(result, c)
		}
	}
	return result
}

func mustParse(f string, typ oid.Oid, s []byte) time.Time {
	str := string(s)

	// check for a 30-minute-offset timezone
	if (typ == oid.T_timestamptz || typ == oid.T_timetz) &&
		str[len(str)-3] == ':' {
		f += ":00"
	}
	t, err := time.Parse(f, str)
	if err != nil {
		errorf("decode: %s", err)
	}
	return t
}

func expect(str, char string, pos int) {
	if c := str[pos : pos+1]; c != char {
		errorf("expected '%v' at position %v; got '%v'", char, pos, c)
	}
}

func mustAtoi(str string) int {
	result, err := strconv.Atoi(str)
	if err != nil {
		errorf("expected number; got '%v'", str)
	}
	return result
}

// The location cache caches the time zones typically used by the client.
type locationCache struct {
	cache map[int]*time.Location
	lock  sync.Mutex
}

// All connections share the same list of timezones. Benchmarking shows that
// about 5% speed could be gained by putting the cache in the connection and
// losing the mutex, at the cost of a small amount of memory and a somewhat
// significant increase in code complexity.
var globalLocationCache *locationCache = newLocationCache()

func newLocationCache() *locationCache {
	return &locationCache{cache: make(map[int]*time.Location)}
}

// Returns the cached timezone for the specified offset, creating and caching
// it if necessary.
func (c *locationCache) getLocation(offset int) *time.Location {
	c.lock.Lock()
	defer c.lock.Unlock()

	location, ok := c.cache[offset]
	if !ok {
		location = time.FixedZone("", offset)
		c.cache[offset] = location
	}

	return location
}

var infinityTsEnabled = false
var infinityTsNegative time.Time
var infinityTsPositive time.Time

const (
	infinityTsEnabledAlready        = "pq: infinity timestamp enabled already"
	infinityTsNegativeMustBeSmaller = "pq: infinity timestamp: negative value must be smaller (before) than positive"
)

/*
 * If EnableInfinityTs is not called, "-infinity" and "infinity" will return
 * []byte("-infinity") and []byte("infinity") respectively, and potentially
 * cause error "sql: Scan error on column index 0: unsupported driver -> Scan pair: []uint8 -> *time.Time",
 * when scanning into a time.Time value.
 *
 * Once EnableInfinityTs has been called, all connections created using this
 * driver will decode Postgres' "-infinity" and "infinity" for "timestamp",
 * "timestamp with time zone" and "date" types to the predefined minimum and
 * maximum times, respectively.  When encoding time.Time values, any time which
 * equals or precedes the predefined minimum time will be encoded to
 * "-infinity".  Any values at or past the maximum time will similarly be
 * encoded to "infinity".
 *
 *
 * If EnableInfinityTs is called with negative >= positive, it will panic.
 * Calling EnableInfinityTs after a connection has been established results in
 * undefined behavior.  If EnableInfinityTs is called more than once, it will
 * panic.
 */
func EnableInfinityTs(negative time.Time, positive time.Time) {
	if infinityTsEnabled {
		panic(infinityTsEnabledAlready)
	}
	if !negative.Before(positive) {
		panic(infinityTsNegativeMustBeSmaller)
	}
	infinityTsEnabled = true
	infinityTsNegative = negative
	infinityTsPositive = positive
}

/*
 * Testing might want to toggle infinityTsEnabled
 */
func disableInfinityTs() {
	infinityTsEnabled = false
}

// This is a time function specific to the Postgres default DateStyle
// setting ("ISO, MDY"), the only one we currently support. This
// accounts for the discrepancies between the parsing available with
// time.Parse and the Postgres date formatting quirks.
func parseTs(currentLocation *time.Location, str string) interface{} {
	switch str {
	case "-infinity":
		if infinityTsEnabled {
			return infinityTsNegative
		}
		return []byte(str)
	case "infinity":
		if infinityTsEnabled {
			return infinityTsPositive
		}
		return []byte(str)
	}

	monSep := strings.IndexRune(str, '-')
	// this is Gregorian year, not ISO Year
	// In Gregorian system, the year 1 BC is followed by AD 1
	year := mustAtoi(str[:monSep])
	daySep := monSep + 3
	month := mustAtoi(str[monSep+1 : daySep])
	expect(str, "-", daySep)
	timeSep := daySep + 3
	day := mustAtoi(str[daySep+1 : timeSep])

	var hour, minute, second int
	if len(str) > monSep+len("01-01")+1 {
		expect(str, " ", timeSep)
		minSep := timeSep + 3
		expect(str, ":", minSep)
		hour = mustAtoi(str[timeSep+1 : minSep])
		secSep := minSep + 3
		expect(str, ":", secSep)
		minute = mustAtoi(str[minSep+1 : secSep])
		secEnd := secSep + 3
		second = mustAtoi(str[secSep+1 : secEnd])
	}
	remainderIdx := monSep + len("01-01 00:00:00") + 1
	// Three optional (but ordered) sections follow: the
	// fractional seconds, the time zone offset, and the BC
	// designation. We set them up here and adjust the other
	// offsets if the preceding sections exist.

	nanoSec := 0
	tzOff := 0

	if remainderIdx < len(str) && str[remainderIdx:remainderIdx+1] == "." {
		fracStart := remainderIdx + 1
		fracOff := strings.IndexAny(str[fracStart:], "-+ ")
		if fracOff < 0 {
			fracOff = len(str) - fracStart
		}
		fracSec := mustAtoi(str[fracStart : fracStart+fracOff])
		nanoSec = fracSec * (1000000000 / int(math.Pow(10, float64(fracOff))))

		remainderIdx += fracOff + 1
	}
	if tzStart := remainderIdx; tzStart < len(str) && (str[tzStart:tzStart+1] == "-" || str[tzStart:tzStart+1] == "+") {
		// time zone separator is always '-' or '+' (UTC is +00)
		var tzSign int
		if c := str[tzStart : tzStart+1]; c == "-" {
			tzSign = -1
		} else if c == "+" {
			tzSign = +1
		} else {
			errorf("expected '-' or '+' at position %v; got %v", tzStart, c)
		}
		tzHours := mustAtoi(str[tzStart+1 : tzStart+3])
		remainderIdx += 3
		var tzMin, tzSec int
		if tzStart+3 < len(str) && str[tzStart+3:tzStart+4] == ":" {
			tzMin = mustAtoi(str[tzStart+4 : tzStart+6])
			remainderIdx += 3
		}
		if tzStart+6 < len(str) && str[tzStart+6:tzStart+7] == ":" {
			tzSec = mustAtoi(str[tzStart+7 : tzStart+9])
			remainderIdx += 3
		}
		tzOff = tzSign * ((tzHours * 60 * 60) + (tzMin * 60) + tzSec)
	}
	var isoYear int
	if remainderIdx < len(str) && str[remainderIdx:remainderIdx+3] == " BC" {
		isoYear = 1 - year
		remainderIdx += 3
	} else {
		isoYear = year
	}
	if remainderIdx < len(str) {
		errorf("expected end of input, got %v", str[remainderIdx:])
	}
	t := time.Date(isoYear, time.Month(month), day,
		hour, minute, second, nanoSec,
		globalLocationCache.getLocation(tzOff))

	if currentLocation != nil {
		// Set the location of the returned Time based on the session's
		// TimeZone value, but only if the local time zone database agrees with
		// the remote database on the offset.
		lt := t.In(currentLocation)
		_, newOff := lt.Zone()
		if newOff == tzOff {
			t = lt
		}
	}

	return t
}

// formatTs formats t into a format postgres understands.
func formatTs(t time.Time) (b []byte) {
	if infinityTsEnabled {
		// t <= -infinity : ! (t > -infinity)
		if !t.After(infinityTsNegative) {
			return []byte("-infinity")
		}
		// t >= infinity : ! (!t < infinity)
		if !t.Before(infinityTsPositive) {
			return []byte("infinity")
		}
	}
	// Need to send dates before 0001 A.D. with " BC" suffix, instead of the
	// minus sign preferred by Go.
	// Beware, "0000" in ISO is "1 BC", "-0001" is "2 BC" and so on
	bc := false
	if t.Year() <= 0 {
		// flip year sign, and add 1, e.g: "0" will be "1", and "-10" will be "11"
		t = t.AddDate((-t.Year())*2+1, 0, 0)
		bc = true
	}
	b = []byte(t.Format(time.RFC3339Nano))

	_, offset := t.Zone()
	offset = offset % 60
	if offset != 0 {
		// RFC3339Nano already printed the minus sign
		if offset < 0 {
			offset = -offset
		}

		b = append(b, ':')
		if offset < 10 {
			b = append(b, '0')
		}
		b = strconv.AppendInt(b, int64(offset), 10)
	}

	if bc {
		b = append(b, " BC"...)
	}
	return b
}

// Parse a bytea value received from the server.  Both "hex" and the legacy
// "escape" format are supported.
func parseBytea(s []byte) (result []byte) {
	if len(s) >= 2 && bytes.Equal(s[:2], []byte("\\x")) {
		// bytea_output = hex
		s = s[2:] // trim off leading "\\x"
		result = make([]byte, hex.DecodedLen(len(s)))
		_, err := hex.Decode(result, s)
		if err != nil {
			errorf("%s", err)
		}
	} else {
		// bytea_output = escape
		for len(s) > 0 {
			if s[0] == '\\' {
				// escaped '\\'
				if len(s) >= 2 && s[1] == '\\' {
					result = append(result, '\\')
					s = s[2:]
					continue
				}

				// '\\' followed by an octal number
				if len(s) < 4 {
					errorf("invalid bytea sequence %v", s)
				}
				r, err := strconv.ParseInt(string(s[1:4]), 8, 9)
				if err != nil {
					errorf("could not parse bytea value: %s", err.Error())
				}
				result = append(result, byte(r))
				s = s[4:]
			} else {
				// We hit an unescaped, raw byte.  Try to read in as many as
				// possible in one go.
				i := bytes.IndexByte(s, '\\')
				if i == -1 {
					result = append(result, s...)
					break
				}
				result = append(result, s[:i]...)
				s = s[i:]
			}
		}
	}

	return result
}

func encodeBytea(serverVersion int, v []byte) (result []byte) {
	if serverVersion >= 90000 {
		// Use the hex format if we know that the server supports it
		result = make([]byte, 2+hex.EncodedLen(len(v)))
		result[0] = '\\'
		result[1] = 'x'
		hex.Encode(result[2:], v)
	} else {
		// .. or resort to "escape"
		for _, b := range v {
			if b == '\\' {
				result = append(result, '\\', '\\')
			} else if b < 0x20 || b > 0x7e {
				result = append(result, []byte(fmt.Sprintf("\\%03o", b))...)
			} else {
				result = append(result, b)
			}
		}
	}

	return result
}

// NullTime represents a time.Time that may be null. NullTime implements the
// sql.Scanner interface so it can be used as a scan destination, similar to
// sql.NullString.
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

type converter struct{}

func (converter) ConvertValue(v interface{}) (driver.Value, error) {
	if driver.IsValue(v) {
		return v, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice:
		var buf bytes.Buffer
		buf.WriteString("{")

		if rv.Len() > 0 {
			var conv func(reflect.Value) string
			dv := rv.Index(0)

			if valuer, ok := dv.Interface().(driver.Valuer); ok {
				val, err := valuer.Value()
				if err != nil {
					return nil, fmt.Errorf("from Value: %v", err)
				}
				dv = reflect.ValueOf(val)
			}

			switch dv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				conv = func(v reflect.Value) string {
					return strconv.FormatInt(v.Int(), 10)
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				conv = func(v reflect.Value) string {
					return strconv.FormatUint(v.Uint(), 10)
				}
			case reflect.Float64:
				conv = func(v reflect.Value) string {
					return strconv.FormatFloat(v.Float(), 'g', -1, 64)
				}
			case reflect.Float32:
				conv = func(v reflect.Value) string {
					return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
				}
			case reflect.Bool:
				conv = func(v reflect.Value) string {
					return strconv.FormatBool(v.Bool())
				}
			case reflect.String:
				conv = func(v reflect.Value) string {
					return v.String()
				}
			case reflect.Struct:
				switch dv.Interface().(type) {
				case time.Time:
					conv = func(v reflect.Value) string {
						return string(formatTs(v.Interface().(time.Time)))
					}
				}
			default:
				return nil, fmt.Errorf("unsupported type %T, a %s", v, rv.Kind())
			}

			for i := 0; i < rv.Len(); i++ {
				buf.WriteString(conv(rv.Index(i)))
				if i < rv.Len()-1 {
					buf.WriteString(",")
				}
			}
		}

		buf.WriteString("}")
		return buf.String(), nil
	}

	// Fallback to the driver default converter for all other types
	return driver.DefaultParameterConverter.ConvertValue(v)
}

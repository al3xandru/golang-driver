package cassandra

// #cgo LDFLAGS: -L/usr/local/lib -lcassandra
// #cgo CFLAGS: -I/usr/local/include
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import (
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

var Epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// Cassandra `timestamp` represents a date plus time,
// encoded as 8 bytes since epoch
type Timestamp struct {
	secondsSinceEpoch int64
}

func NewTimestampFromTime(t time.Time) Timestamp {
	return Timestamp{int64(t.Sub(Epoch).Seconds())}
}

func NewTimestamp(secondsFromEpoch int64) Timestamp {
	return Timestamp{secondsFromEpoch}
}

func (t Timestamp) Time() time.Time {
	return Epoch.Add(time.Duration(t.secondsSinceEpoch) * time.Second)
}

func (t Timestamp) String() string {
	return t.Time().String()
}

func (t Timestamp) NativeString() string {
	// FIXME: should return the value as represented in CQL
	return fmt.Sprintf("%d", t.secondsSinceEpoch)
}

func (t Timestamp) Raw() int64 {
	return t.secondsSinceEpoch
}

// Cassandra Date is a 32-bit unsigned integer representing
// the number of days with Epoch (1970-1-1) at the center of the range
type Date struct {
	days uint32
}

// Create a new Date.
func NewDate(year int, month time.Month, day int) Date {
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	d := date.Sub(Epoch)
	var days int64 = int64(d.Hours()) / 24
	var udays = uint32(days + math.MaxInt32 + 1)

	return Date{udays}
}

var datePatterns = []string{
	"2006-01-02",
	"2006-01-2",
	"2006-1-02",
	"2006-1-2",
}

// Creates a new Date value from the string representation.
// The accepted formats are:
// * yyyy-mm-dd
// * yyyy-m-d
// * yyyy-mm-d
// * yyyy-m-dd
// If the value cannot be parsed to a valid date, this function
// return a non-nil error
func ParseDate(s string) (Date, error) {
	var t time.Time
	var err error
	for _, p := range datePatterns {
		t, err = time.Parse(p, s)
		if err == nil {
			break
		}
	}
	if err != nil {
		return Date{}, err
	}
	return NewDate(t.Year(), t.Month(), t.Day()), nil
}

// Only the year, month, day part are set in the returned time.Time
func (d Date) Time() time.Time {
	var v int64 = int64(d.days) - math.MaxInt32 - 1
	return Epoch.Add(time.Duration(v*24) * time.Hour)
}

func (d Date) Raw() uint32 {
	return d.days
}

func (d Date) String() string {
	return d.Time().Format("2006-01-02")
}

// Returns a representation that can be used directly in CQL
func (d Date) NativeString() string {
	return fmt.Sprintf("'%d'", d.days)
}

// Cassandra `time` type represents a time of day
// with no date (and no notion of time zone)
type Time int64

var nanosInADay int64 = 24 * int64(time.Hour)

func NewTime(hours, minutes, seconds, nanos uint) (Time, error) {
	var nanotime int64 = int64(hours)*int64(time.Hour) +
		int64(minutes)*int64(time.Minute) +
		int64(seconds)*int64(time.Second) +
		int64(nanos)
	if nanotime < 0 || nanotime > nanosInADay {
		return 0, fmt.Errorf("Time value must be bigger than 0 and less than the number of nanoseconds in a day (%d)", nanosInADay)
	}
	return Time(nanotime), nil
}

func ParseTime(str string) (Time, error) {
	parts := strings.Split(str, ".")
	hms := strings.Split(parts[0], ":")
	if len(hms) != 3 {
		return 0, fmt.Errorf("Time must be in format hh:mm:ss.nnnnnnnnn")
	}
	var nanotime int64
	n, err := strconv.ParseInt(hms[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Time must be in format hh:mm:ss.nnnnnnnnn")
	}
	nanotime += n * int64(time.Hour)
	n, err = strconv.ParseInt(hms[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Time must be in format hh:mm:ss.nnnnnnnnn")
	}
	nanotime += n * int64(time.Minute)
	n, err = strconv.ParseInt(hms[2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Time must be in format hh:mm:ss.nnnnnnnnn")
	}
	nanotime += n * int64(time.Second)
	if len(parts) > 1 {
		padded := parts[1] + strings.Repeat("0", (9-len(parts[1])))
		fmt.Printf(padded)
		n, err = strconv.ParseInt(padded, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("Time must be in format hh:mm:ss.nnnnnnnnn")
		}
		nanotime += n
	}
	return Time(nanotime), nil
}

func (t Time) Hours() uint {
	return uint(math.Floor(float64(t) / float64(time.Hour)))

}

func (t Time) Minutes() uint {
	minutes := math.Floor(float64(t) / float64(time.Minute))
	return uint(math.Remainder(minutes, 60))
}

func (t Time) Seconds() uint {
	seconds := math.Floor(float64(t) / float64(time.Second))
	return uint(math.Remainder(seconds, 60))
}

func (t Time) Nanoseconds() uint {
	return uint(math.Remainder(float64(t), float64(time.Second)))
}

func (t Time) Raw() int64 {
	return int64(t)
}

func (t Time) String() string {
	return fmt.Sprintf("%02d:%02d:%02d.%d", t.Hours(), t.Minutes(), t.Seconds(),
		t.Nanoseconds())
}

func (t Time) NativeString() string {
	return fmt.Sprintf("'%s'", t.String())
}

// Used to represent both a Cassandra `uuid` (UUID v4)
// and `timeuuid` (UUID v1).
//
// Inspired by:
// * https://github.com/satori/go.uuid
// * https://github.com/pborman/uuid
type UUID [16]byte

func ParseUUID(s string) (uuid UUID, err error) {
	if len(s) != 36 {
		return uuid, fmt.Errorf("Invalid UUID format: %s", s)
	}

	for _, idx := range []int{8, 13, 18, 23} {
		if s[idx] != '-' {
			return uuid, fmt.Errorf("Invalid UUID format: %s", s)
		}
	}

	b := []byte(s)
	u := uuid[:]
	_, err = hex.Decode(u[0:4], b[0:8])
	_, err = hex.Decode(u[4:6], b[9:13])
	_, err = hex.Decode(u[6:8], b[14:18])
	_, err = hex.Decode(u[8:10], b[19:23])
	_, err = hex.Decode(u[10:], b[24:])
	return
}

func (u UUID) Version() uint {
	return uint(u[6] >> 4)
}

func (u UUID) String() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		u[:4], u[4:6], u[6:8], u[8:10], u[10:])
}

func (u UUID) NativeString() string {
	return u.String()
}

func (u UUID) Raw() [16]byte {
	return [16]byte(u)
}

// Utility method allowing to mark an array, slice, or map as a value
// to be written as Cassandra `set` from simple statements (in which
// automatic conversions wouldn't recognize those types as potential
// sets without incurring a significant penalty)
func Set(val interface{}) setmarker {
	return setmarker{val}
}

type setmarker struct {
	value interface{}
}

// A Decimal type corresponding to the Cassandra decimal data type.
// The internal representation of the decimal is an arbitrary precision
// integer unscaled balue and a 32-bit integer scale. Thus the value
// is equal to (unscaled * 10 ^ (-scale))
type Decimal struct {
	Value *big.Int
	Scale int32
}

func ParseDecimal(val string) (*Decimal, error) {
	if val == "" {
		return nil, fmt.Errorf("val must non empty")
	}
	parts := strings.Split(val, ".")
	if len(parts) > 2 {
		return nil, fmt.Errorf("%s is not a valid decimal (too many .)", val)
	}

	scale := 0
	if len(parts) == 2 {
		scale = len(parts[1])
	}
	if scale < math.MinInt32 || scale > math.MaxInt32 {
		return nil, fmt.Errorf("%s is not a valid decimal (fractional part too large)", val)
	}
	bigint := big.NewInt(0)
	bigint, ok := bigint.SetString(strings.Join(parts, ""), 10)
	if !ok {
		return nil, fmt.Errorf("val is not a valid decimal (%s)", val)
	}

	return &Decimal{bigint, int32(scale)}, nil
}

func NewDecimal(val int64, scale int32) *Decimal {
	return &Decimal{big.NewInt(val), scale}
}

func (d Decimal) String() string {
	s := d.Value.String()
	pos := len(s) - int(d.Scale)
	return fmt.Sprintf("%s.%s", s[0:pos], s[pos:])
}

func (d Decimal) NativeString() string {
	return d.String()
}

// A Tuple type corresponding to the Cassandra tuple data type.
type Tuple struct {
	kind   CassType
	values []interface{}
}

func NewTuple(t CassType, args ...interface{}) *Tuple {
	tuple := new(Tuple)
	tuple.kind = t
	tuple.values = make([]interface{}, len(t.SubTypes))
	if len(args) > 0 {
		copy(tuple.values, args)
	}

	return tuple
}

func (tuple Tuple) Kind() CassType {
	return tuple.kind
}

func (tuple Tuple) Values() []interface{} {
	return tuple.values
}

func (tuple Tuple) Len() int {
	return len(tuple.kind.SubTypes)
}

// Sets the value at the given index and returns the same
// pointer to the Tuple so multiple Set operations can be chained.
// This method panics if the provided index falls outside the length
// of the tuple
func (tuple *Tuple) Set(index int, value interface{}) *Tuple {
	if index < 0 || index >= tuple.Len() {
		panic(fmt.Sprintf("Cannot set value at index %d in a tuple %s which has only %d values",
			index, tuple.kind.Name(), tuple.Len()))
	}
	tuple.values[index] = value

	return tuple
}

func (tuple *Tuple) SetValues(values ...interface{}) error {
	if tuple.Len() < len(values) {
		return fmt.Errorf("Cannot set %d values in a tuple %s which has only %d values",
			len(values), tuple.kind.Name(), tuple.Len())
	}
	copy(tuple.values, values)

	return nil
}

func (tuple Tuple) Get(index int) interface{} {
	if index < 0 || index >= tuple.Len() {
		panic(fmt.Sprintf("Tuple %s has only %d values", tuple.kind.Name(), tuple.Len()))
	}

	return tuple.values[index]
}

func (tuple Tuple) String() string {
	names := make([]string, tuple.Len())
	for i, st := range tuple.kind.SubTypes {
		names[i] = "%v " + st.Name()
	}
	format := "tuple<" + strings.Join(names, ", ") + ">"
	return fmt.Sprintf(format, tuple.values...)
}

func (tuple Tuple) NativeString() string {
	if tuple.Len() == 0 {
		return "()"
	}
	format := "(" + strings.Repeat("%v, ", tuple.Len()-1) + "%v)"
	return fmt.Sprintf(format, tuple.values...)
}

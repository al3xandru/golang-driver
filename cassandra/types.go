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
	"strconv"
	"strings"
	"time"
)

// Cassandra `timestamp` represents a date plus time,
// encoded as 8 bytes since epoch
type Timestamp int64

// Cassandra Date is a 32-bit unsigned integer representing
// the number of days with Epoch (1970-1-1) at the center of the range
type Date struct {
	Days uint32
}

// Only the year, month, day part are set in the returned time.Time
func (d *Date) Time() time.Time {
	var v int64 = int64(d.Days) - math.MaxInt32
	return Epoch.Add(time.Duration(v*24) * time.Hour)
}

func (d *Date) String() string {
	return d.Time().Format("2006-01-02")
}

var Epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// Create a new Date.
func NewDate(year int, month time.Month, day int) *Date {
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	d := date.Sub(Epoch)
	var days int64 = int64(d.Hours()) / 24
	var udays = uint32(days + math.MaxInt32 + 1)

	return &Date{udays}
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

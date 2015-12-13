package cassandra

import (
	"fmt"
	"math"
	"time"
)

// Cassandra Date is a 32-bit unsigned integer representing
// the number of days with Epoch (1970-1-1) at the center of the range
type Date struct {
	Days uint32
}

func (d *Date) Time() time.Time {
	var v int64 = int64(d.Days) - math.MaxInt32
	return Epoch.Add(time.Duration(v*24) * time.Hour)
}

func (d *Date) String() string {
	return d.Time().Format("2006-01-02")
}

var Epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

func NewDate(year int, month time.Month, day int) *Date {
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	d := date.Sub(Epoch)
	var days int64 = int64(d.Hours()) / 24
	var udays = uint32(days + math.MaxInt32 + 1)
	fmt.Printf("days: %d\n", udays)
	return &Date{udays}
}

type Time struct {
	Nanos int64
}

func (t *Time) Duration() time.Duration {
	return time.Duration(t.Nanos) * time.Nanosecond
}

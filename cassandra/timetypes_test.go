package cassandra_test

import (
	"golang-driver/cassandra"
	"testing"
)

func TestTime(t *testing.T) {
	t1, err := cassandra.NewTime(25, 0, 0, 0)
	if err == nil {
		t.Error("Time should always be less than 24")
	}
	t1, err = cassandra.NewTime(9, 10, 11, 345678900)
	if err != nil {
		t.Fatal(err)
	}
	t2, err := cassandra.ParseTime("09:10:11.3456789")

	if err != nil {
		t.Fatal(err)
	}
	if t1 != t2 {
		t.Fatalf("%d != %d", t1, t2)
	}

	if t2.Hours() != 9 {
		t.Errorf("Hours: 9 != %d", t2.Hours())
	}
	if t2.Minutes() != 10 {
		t.Errorf("Minutes: 10 != %d", t2.Minutes())
	}
	if t2.Seconds() != 11 {
		t.Errorf("Seconds: 11 != %d", t2.Seconds())
	}
	if t2.Nanoseconds() != 345678900 {
		t.Errorf("Nanos: 345678900 != %d", t2.Nanoseconds())
	}
}

package cassandra_test

import (
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"math"
	"testing"
	"time"
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

func TestTimestamp(t *testing.T) {
	ts1 := &cassandra.Timestamp{1450606299}
	ts2 := cassandra.NewTimestamp(time.Date(2015, 12, 20, 10, 11, 39, 0, time.UTC))

	if !ts1.Time().Equal(ts2.Time()) {
		t.Fatalf("%d != %d (%s != %s)", ts1.SecondsSinceEpoch, ts2.SecondsSinceEpoch,
			ts1.Time().String(), ts2.Time().String())
	}
}

func TestDate(t *testing.T) {
	d1 := cassandra.NewDate(2015, 12, 20)
	tDay := time.Date(2015, 12, 20, 0, 0, 0, 0, time.UTC)

	if !tDay.Equal(d1.Time()) {
		t.Errorf("%s != %s", tDay, d1.Time())
	}

	d2 := cassandra.NewDate(1970, 1, 1)
	if float64(d2.Days) != math.Pow(2, 31) {
		t.Errorf("Center %d != %d", math.Pow(2, 31), d2.Days)
	}

	d2, err := cassandra.ParseDate("2015-12-20")
	if err != nil {
		t.Fatal(err)
	}
	if !d1.Time().Equal(d2.Time()) {
		t.Errorf("%d = %d (%s != %s)", d1.Days, d2.Days,
			d1.Time(), d2.Time())
	}
}

func TestTimeTypes(t *testing.T) {
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(timetypesSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manually golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(timetypesCleanup)
	rows, err := session.Execute("SELECT td, tt, ts FROM golang_driver.timetypes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	expectedTime, _ := cassandra.NewTime(13, 29, 7, 234000000)
	var tt cassandra.Time
	var ts cassandra.Timestamp
	var td cassandra.Date
	var tAsInt64 int64
	var dAsUint32 uint32
	var tsAsInt64 int64

	if rows.Next() {
		if err := rows.Scan(&td, &tt, &ts); err != nil {
			t.Fatal(err)
		}
		if td.String() != "2015-08-23" {
			t.Errorf("Date '2015-08-23' != %s", td.String())
		}
		if tt != expectedTime {
			t.Errorf("%02d:%02d:%02d.%d", tt.Hours(), tt.Minutes(), tt.Seconds(), tt.Nanoseconds())
		}
		if ts.SecondsSinceEpoch != 1450606299 {
			t.Errorf("Timestamp 1450606299 != %d (%s)", ts.SecondsSinceEpoch, ts.Time())
		}
		if err := rows.Scan(&dAsUint32, &tAsInt64, &tsAsInt64); err != nil {
			t.Fatal(err)
		}
		if dAsUint32 != 2147500318 {
			t.Errorf("Date 2147500318 != %d", dAsUint32)
		}
		if tAsInt64 != 48547234000000 {
			t.Errorf("Time 13:29:07.234 (48547234000000) != %d", tAsInt64)
		}
		if tsAsInt64 != 1450606299 {
			t.Errorf("Timestamp 47234000000 != %d", tsAsInt64)
		}
	}
}

var (
	timetypesSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		`CREATE TABLE IF NOT EXISTS golang_driver.timetypes(id int PRIMARY KEY, 
		td date, tt time, ts timestamp)`,
		"INSERT INTO golang_driver.timetypes (id, td, tt, ts) VALUES (1, '2015-08-23', '13:29:07.234', 1450606299)",
	}

	timetypesCleanup = []string{
		"DROP TABLE golang_driver.timetypes",
	}
)

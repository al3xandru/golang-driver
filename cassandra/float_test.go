package cassandra_test

import (
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"testing"
)

func TestFloatTypes(t *testing.T) {
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(floatSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manually golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(floatCleanup)

	testReadFloats(t, session)
	testMissingFloats(t, session)
}

func testReadFloats(t *testing.T, session *cassandra.Session) {
	rows, err := session.Exec("SELECT flt, dbl FROM golang_driver.floats WHERE id = 1")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()

	var flt float32
	var dbl float64

	if !rows.Next() {
		t.Error("expected 1 result")
		return
	}
	if err := rows.Scan(&flt, &dbl); err != nil {
		t.Error(err)
		return
	}
	if flt != 3.141592 {
		t.Errorf("float should be 3.141592 != %f", flt)
	}
	if dbl != 3.141592653589 {
		t.Errorf("double should be 3.141592653589 != %f", dbl)
	}
}

func testMissingFloats(t *testing.T, session *cassandra.Session) {
	rows, err := session.Exec("SELECT flt, dbl FROM golang_driver.floats WHERE id = 2")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()

	var flt float32
	var dbl float64

	if !rows.Next() {
		t.Error("expected 1 result")
		return
	}
	if err := rows.Scan(&flt, &dbl); err != nil {
		t.Error(err)
		return
	}

	if flt != 0 {
		t.Errorf("default float should be 0 != %f", flt)
	}
	if dbl != 0 {
		t.Errorf("default int32 should be 0 != %f", dbl)
	}
	// t.Fail()
}

var (
	floatSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		`CREATE TABLE IF NOT EXISTS golang_driver.floats (id int PRIMARY KEY, flt float, dbl double)`,
		`INSERT INTO golang_driver.floats (id, flt, dbl) VALUES (1, 3.141592, 3.141592653589)`,
		`INSERT INTO golang_driver.floats (id) VALUES (2)`,
	}

	floatCleanup = []string{
	// "DROP TABLE golang_driver.floats",
	}
)

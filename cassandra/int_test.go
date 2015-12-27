package cassandra_test

import (
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"testing"
)

func TestIntTypes(t *testing.T) {
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(intsSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manually golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(intsCleanup)

	testReadValues(t, session)
	testMissingValues(t, session)
}

func testReadValues(t *testing.T, session *cassandra.Session) {
	rows, err := session.Execute("SELECT bgnt, nt, smllnt, tnnt, nt FROM golang_driver.ints WHERE id = 1")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()

	var i64 int64
	var i32 int32
	var i16 int16
	var i8 int8
	var nt int

	if !rows.Next() {
		t.Error("expected 1 result")
		return
	}
	if err := rows.Scan(&i64, &i32, &i16, &i8, &nt); err != nil {
		t.Error(err)
		return
	}
	// int types
	if i64 != 9223372036854770000 {
		t.Errorf("bigint should be 9223372036854770000 != %d", i64)
	}
	if i32 != -2147483000 {
		t.Errorf("int32 should be -2147483000 != %d", i32)
	}
	if i16 != 32000 {
		t.Errorf("smallint should be 32000 != %d", i16)
	}
	if i8 != 126 {
		t.Errorf("tinyint should be 126 != %d", i8)
	}
	if nt != -2147483000 {
		t.Errorf("int32 should be -2147483000 != %d", nt)
	}
	// t.Fail()
}

func testMissingValues(t *testing.T, session *cassandra.Session) {
	rows, err := session.Execute("SELECT bgnt, nt, smllnt, tnnt, nt FROM golang_driver.ints WHERE id = 2")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()

	var i64 int64
	var i32 int32
	var i16 int16
	var i8 int8
	var nt int

	if !rows.Next() {
		t.Error("expected 1 result")
		return
	}
	if err := rows.Scan(&i64, &i32, &i16, &i8, &nt); err != nil {
		t.Error(err)
		return
	}
	// int types
	if i64 != 0 {
		t.Errorf("default bigint should be 0 != %d", i64)
	}
	if i32 != 0 {
		t.Errorf("default int32 should be 0 != %d", i32)
	}
	if i16 != 0 {
		t.Errorf("smallint should be 0 != %d", i16)
	}
	if i8 != 0 {
		t.Errorf("tinyint should be 0 != %d", i8)
	}
	if nt != 0 {
		t.Errorf("default int should be 0 != %d", nt)
	}
	// t.Fail()
}

var (
	intsSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		`CREATE TABLE IF NOT EXISTS golang_driver.ints (id int PRIMARY KEY, 
		bgnt bigint, nt int, smllnt smallint, tnnt tinyint)`,
		`INSERT INTO golang_driver.ints (id, bgnt, nt, smllnt, tnnt) 
		VALUES (1, 9223372036854770000, -2147483000, 32000, 126)`,
		`INSERT INTO golang_driver.ints (id) VALUES (2)`,
	}

	intsCleanup = []string{
		"DROP TABLE golang_driver.ints",
	}
)

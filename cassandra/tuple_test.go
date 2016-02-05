package cassandra_test

import (
	"fmt"
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"testing"
)

func TestNewTupleFailures(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(error); ok {
				t.Errorf("expected panic but got nothing")
			}
		}
	}()

	tuple := cassandra.NewTuple(cassandra.CTuple.Specialize(cassandra.CInt, cassandra.CInt))
	tuple.Set(2, 42)
	tuple.Get(2)
}

func TestEmptyTupleGet(t *testing.T) {
	tuple := cassandra.NewTuple(cassandra.CTuple.Specialize(cassandra.CInt, cassandra.CInt))
	val := tuple.Get(0)
	if val != nil {
		t.Errorf("unpopulated tuple must return nil")
	}
}

func TestNewTuple(t *testing.T) {
	tuple := cassandra.NewTuple(cassandra.CTuple.Specialize(cassandra.CInt, cassandra.CInt),
		10, 20)
	if _, ok := tuple.Get(1).(bool); ok {
		t.Errorf("cannot retrieve value %d as bool from %s", 1, tuple.Kind().String())
	}
	if val, ok := tuple.Get(1).(int); !ok {
		t.Errorf("retrieving value %d from %s should work", 1, tuple.Kind().String())
	} else if 20 != val {
		t.Errorf("%d != %d (expected)", val, 20)
	}
}

func TestTuple(t *testing.T) {
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(tupleSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manually golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(tupleCleanup)

	testSelectTuple(t, session, 1,
		[]interface{}{"(true, 1, abc)", "tuple<true boolean, 1 int, abc varchar>", 1, "abc"})
	testInsertTupleUsingStatement(t, session)
	testInsertTupleUsingPreparedStatement(t, session)

	// more tests
	testReadTuple(t, session, "tupleints", 1,
		cassandra.CTuple.Specialize(cassandra.CTinyInt, cassandra.CSmallInt, cassandra.CInt, cassandra.CBigInt, cassandra.CVarint),
		"tuple<1 tinyint, 2 smallint, 3 int, 8876543210987654321 bigint, 123456789012345678901234567890 varint>")
	testReadTuple(t, session, "tuplefloats", 1,
		cassandra.CTuple.Specialize(cassandra.CFloat, cassandra.CDouble, cassandra.CDecimal),
		"tuple<1.1 float, 2.2 double, 42.42 decimal>")
	testReadTuple(t, session, "tupletimes", 1,
		cassandra.CTuple.Specialize(cassandra.CTimestamp, cassandra.CTime, cassandra.CDate),
		"tuple<2015-12-20 10:11:39 +0000 UTC timestamp, 13:29:07.234000000 time, 2015-08-23 date>")
	testReadTuple(t, session, "tupleuuids", 1,
		cassandra.CTuple.Specialize(cassandra.CUuid, cassandra.CTimeuuid),
		"tuple<f0d07136-62f9-4d18-a6ce-cd5f4beb4348 uuid, 9ecc5dd0-a548-11e5-83b1-dfa924dad615 timeuuid>")
	testReadTuple(t, session, "tupleblobinet", 1,
		cassandra.CTuple.Specialize(cassandra.CBlob, cassandra.CInet),
		"tuple<[0 0 0 3] blob, 4.4.4.4 inet>")
	// FIXME: this looks a bit unexpected
	testReadTuple(t, session, "tupletexts", 1,
		cassandra.CTuple.Specialize(cassandra.CAscii, cassandra.CVarchar, cassandra.CVarchar),
		"tuple<asciiascii ascii, texttext varchar, varcharvarchar varchar>")
}

func testSelectTuple(t *testing.T, s *cassandra.Session, id int, expected []interface{}) {
	rows, err := s.Exec("SELECT tpl from golang_driver.tuples WHERE id = ?", id)
	if err != nil {
		t.Error(err)
		return
	}
	if !rows.Next() {
		t.Errorf("expected 1 row")
		return
	}
	var tuple cassandra.Tuple
	if err := rows.Scan(&tuple); err != nil {
		t.Error(err)
		return
	}
	if tuple.NativeString() != expected[0] {
		t.Errorf("%s != %v", tuple.NativeString(), expected[0])
	}
	if tuple.String() != expected[1] {
		t.Errorf("string %s != %v", tuple.String(), expected[1])
	}
	val := tuple.Get(1)
	if act, ok := val.(int); !ok {
		t.Errorf("2nd value must be int in %s, got %T", tuple.Kind().String(), val)
	} else if act != expected[2] {
		t.Errorf("%d != %d", act, expected[2])
	}
	val = tuple.Get(2)
	if act, ok := val.(string); !ok {
		t.Errorf("3rd value must be string in %s, got %T", tuple.Kind().String(), val)
	} else if act != expected[3] {
		t.Errorf("%s != %s", act, expected[3])
	}
}

func testInsertTupleUsingStatement(t *testing.T, s *cassandra.Session) {
	// t.Skip()
	tupleType := cassandra.CTuple.Specialize(cassandra.CBoolean, cassandra.CInt, cassandra.CText)
	tuple := cassandra.NewTuple(tupleType, false, 101, "statement")
	if _, err := s.Exec("INSERT INTO golang_driver.tuples (id, tpl) VALUES (?, ?)",
		101, tuple); err != nil {
		t.Error(err)
		return
	}

	testSelectTuple(t, s, 101,
		[]interface{}{"(false, 101, statement)",
			"tuple<false boolean, 101 int, statement varchar>",
			101,
			"statement"})
}

func testInsertTupleUsingPreparedStatement(t *testing.T, s *cassandra.Session) {
	pstmt, err := s.Prepare("INSERT INTO golang_driver.tuples (id, tpl) VALUES (?, ?)")
	if err != nil {
		t.Error(err)
		return
	}

	tupleType := cassandra.CTuple.Specialize(cassandra.CBoolean, cassandra.CInt, cassandra.CText)
	tuple := cassandra.NewTuple(tupleType, false, 1001, "prepared")
	if _, err = pstmt.Exec(1001, tuple); err != nil {
		t.Error(err)
		return
	}

	testSelectTuple(t, s, 1001,
		[]interface{}{"(false, 1001, prepared)",
			"tuple<false boolean, 1001 int, prepared varchar>",
			1001,
			"prepared"})
}

var (
	tupleSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		"CREATE TABLE IF NOT EXISTS golang_driver.tuples (id int PRIMARY KEY, tpl tuple<boolean, int, text>)",
		"INSERT INTO golang_driver.tuples (id, tpl) VALUES (1, (true, 1, 'abc'))",
		"CREATE TABLE IF NOT EXISTS golang_driver.tupleints (id int PRIMARY KEY, tpl tuple<tinyint, smallint, int, bigint, varint>)",
		"INSERT INTO golang_driver.tupleints (id, tpl) VALUES (1, (1, 2, 3, 8876543210987654321, 123456789012345678901234567890))",
		"CREATE TABLE IF NOT EXISTS golang_driver.tuplefloats (id int PRIMARY KEY, tpl tuple<float, double, decimal>)",
		"INSERT INTO golang_driver.tuplefloats (id, tpl) VALUES (1, (1.1, 2.2, 42.42))",
		"CREATE TABLE IF NOT EXISTS golang_driver.tupletimes (id int PRIMARY KEY, tpl tuple<timestamp, time, date>)",
		"INSERT INTO golang_driver.tupletimes (id, tpl) VALUES (1, (1450606299, '13:29:07.234', '2015-08-23'))",
		"CREATE TABLE IF NOT EXISTS golang_driver.tupleuuids (id int PRIMARY KEY, tpl tuple<uuid, timeuuid>)",
		"INSERT INTO golang_driver.tupleuuids (id, tpl) VALUES (1, (f0d07136-62f9-4d18-a6ce-cd5f4beb4348, 9ecc5dd0-a548-11e5-83b1-dfa924dad615))",
		"CREATE TABLE IF NOT EXISTS golang_driver.tupletexts (id int PRIMARY KEY, tpl tuple<ascii, text, varchar>)",
		"INSERT INTO golang_driver.tupletexts (id, tpl) VALUES (1, ('asciiascii', 'texttext', 'varcharvarchar'))",
		"CREATE TABLE IF NOT EXISTS golang_driver.tupleblobinet (id int PRIMARY KEY, tpl tuple<blob, inet>)",
		"INSERT INTO golang_driver.tupleblobinet (id, tpl) VALUES (1, (intasblob(3), '4.4.4.4'))",
	}

	tupleCleanup = []string{
		"DROP TABLE golang_driver.tuples",
		"DROP TABLE golang_driver.tupleints",
		"DROP TABLE golang_driver.tuplefloats",
		"DROP TABLE golang_driver.tupletimes",
		"DROP TABLE golang_driver.tupleuuids",
		"DROP TABLE golang_driver.tupletexts",
		"DROP TABLE golang_driver.tupleblobinet",
	}
)

func testReadTuple(t *testing.T, s *cassandra.Session, table string, rowId int, expected ...interface{}) {
	stmt := fmt.Sprintf("SELECT tpl from golang_driver.%s WHERE id = ?", table)
	t.Logf("testReadTuple(%s, %d)", table, rowId)
	rows, err := s.Exec(stmt, rowId)
	if err != nil {
		t.Error(err)
		return
	}
	if !rows.Next() {
		t.Errorf("expecting 1 result for %s with id %d", table, rowId)
		return
	}
	var tpl cassandra.Tuple

	if err := rows.Scan(&tpl); err != nil {
		t.Error(err)
		return
	}
	kind := tpl.Kind()
	expectedKind := expected[0].(cassandra.CassType)
	if !kind.Equals(expectedKind) {
		t.Errorf("actual %s != %s expected (%s)", kind.String(),
			expectedKind.String(), table)
	}

	if len(expected) < 2 {
		return
	}

	if tpl.String() != expected[1] {
		t.Errorf("%s != %s (%s)", tpl.String(), expected[1], table)
	}
}

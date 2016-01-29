package cassandra_test

import (
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

	tuple := cassandra.NewTuple(cassandra.CASS_TUPLE.Subtype(cassandra.CASS_INT, cassandra.CASS_INT))
	tuple.Set(2, 42)
	tuple.Get(2)
}

func TestEmptyTupleGet(t *testing.T) {
	tuple := cassandra.NewTuple(cassandra.CASS_TUPLE.Subtype(cassandra.CASS_INT, cassandra.CASS_INT))
	val := tuple.Get(0)
	if val != nil {
		t.Errorf("unpopulated tuple must return nil")
	}
}

func TestNewTuple(t *testing.T) {
	tuple := cassandra.NewTuple(cassandra.CASS_TUPLE.Subtype(cassandra.CASS_INT, cassandra.CASS_INT),
		10, 20)
	if _, ok := tuple.Get(1).(bool); ok {
		t.Errorf("cannot retrieve value %d as bool from %s", 1, tuple.Kind.Name())
	}
	if val, ok := tuple.Get(1).(int); !ok {
		t.Errorf("retrieving value %d from %s should work", 1, tuple.Kind.Name())
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

	testSelectTuple(t, session, 1)
	testInsertTupleUsingStatement(t, session)
	testInsertTupleUsingPreparedStatement(t, session)
}

func testSelectTuple(t *testing.T, s *cassandra.Session, id int) {
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
	t.Logf("Tuple %s: %v", tuple.Kind.Name(), tuple.Values)
	t.Logf(tuple.String())
}

func testInsertTupleUsingStatement(t *testing.T, s *cassandra.Session) {
	t.Skip()
	tuple := cassandra.NewTuple(cassandra.CASS_TUPLE.Subtype(cassandra.CASS_BOOLEAN, cassandra.CASS_INT, cassandra.CASS_TEXT), false, 42, "statement")
	if _, err := s.Exec("INSERT INTO golang_driver.tuples (id, tpl) VALUES (?, ?)",
		100, tuple); err != nil {
		t.Error(err)
		return
	}

	testSelectTuple(t, s, 101)
}

func testInsertTupleUsingPreparedStatement(t *testing.T, s *cassandra.Session) {
	t.Skip()
	pstmt, err := s.Prepare("INSERT INTO golang_driver.tuples (id, tpl) VALUES (?, ?)")
	if err != nil {
		t.Error(err)
		return
	}

	tuple := cassandra.NewTuple(cassandra.CASS_TUPLE.Subtype(cassandra.CASS_BOOLEAN, cassandra.CASS_INT, cassandra.CASS_TEXT), false, 42, "statement")
	if _, err = pstmt.Exec(1001, tuple); err != nil {
		t.Error(err)
		return
	}

	testSelectTuple(t, s, 1001)
}

var (
	tupleSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		`CREATE TABLE IF NOT EXISTS golang_driver.tuples (id int PRIMARY KEY, tpl tuple<boolean, int, text>)`,
		`INSERT INTO golang_driver.tuples (id, tpl) VALUES (1, (true, 1, 'abc'))`,
	}

	tupleCleanup = []string{
	// "DROP TABLE golang_driver.bignumbers",
	}
)

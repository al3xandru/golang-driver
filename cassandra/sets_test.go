package cassandra_test

import (
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"testing"
)

func TestSets(t *testing.T) {
	// t.Skipf("Collections do not work yet")
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(setsSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manuallly golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(setsCleanup)

	testSelectSets(t, session)
	testInsertSetUsingPreparedStatement(t, session)
	testInsertSetUsingStatement(t, session)
	testInsertSetUsingStatementWithSet(t, session)
	testWeirdBehaviorForSets(t, session)
}

func testSelectSets(t *testing.T, session *cassandra.Session) {
	rows, err := session.Exec("SELECT tset FROM golang_driver.sets")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()

	if !rows.Next() {
		t.Error("There should be at least 1 row.")
		return
	}
	var set map[cassandra.UUID]bool

	if err := rows.Scan(&set); err != nil {
		t.Error(err)
	}
	if len(set) != 5 {
		t.Errorf("tset set<timeuuid> should have 5 elements; actual %d (%v)",
			len(set), set)
	}
	uuid, _ := cassandra.ParseUUID("97294c90-a549-11e5-83b1-dfa924dad615")
	if !set[uuid] {
		t.Errorf("uuid %s should be in the set %v", uuid.String(), set)
	}
}

func testInsertSetUsingPreparedStatement(t *testing.T, session *cassandra.Session) {
	prepStmt, err := session.Prepare("INSERT INTO golang_driver.sets (id, intset) VALUES (?, ?)")
	if err != nil {
		t.Error(err)
		return
	}

	if _, err = prepStmt.Exec(20, []int{20, 21, 22}); err != nil {
		t.Error(err)
	}
	if _, err = prepStmt.Exec(30, map[int]bool{30: true, 31: true, 32: true}); err != nil {
		t.Error(err)
	}
	if _, err = prepStmt.Exec(21, cassandra.Set([]int{20, 21, 22})); err != nil {
		t.Error(err)
	}
	if _, err = prepStmt.Exec(31, cassandra.Set(map[int]bool{30: true, 31: true, 32: true})); err != nil {
		t.Error(err)
	}
}

func testInsertSetUsingStatement(t *testing.T, session *cassandra.Session) {
	var err error

	if _, err = session.Exec("INSERT INTO golang_driver.sets (id, intset) VALUES (?, ?)",
		5, map[int]bool{50: true, 51: true, 52: true}); err == nil {
		t.Error("converting from map[int]bool to set<int> should not work")
	} else {
		t.Log(err)
	}
}

func testInsertSetUsingStatementWithSet(t *testing.T, session *cassandra.Session) {
	var err error

	if _, err = session.Exec("INSERT INTO golang_driver.sets (id, intset) VALUES (?, ?)",
		6, cassandra.Set([]int{60, 61, 62})); err != nil {
		t.Error(err)
	}
	if _, err = session.Exec("INSERT INTO golang_driver.sets (id, intset) VALUES (?, ?)",
		7, cassandra.Set(map[int]bool{70: true, 71: true, 72: true})); err != nil {
		t.Error(err)
	}
	// Set() can wrap only arrays, slices, maps
	if _, err = session.Exec("INSERT INTO golang_driver.sets (id, intset) VALUES (?, ?)",
		8, cassandra.Set("a string")); err == nil {
		t.Error("cassandra.Set() can wrap anything, but it should result in an error")
	} else {
		t.Log(err)
	}
}

func testWeirdBehaviorForSets(t *testing.T, session *cassandra.Session) {
	t.Skipf("This Set test is misbehaving for unclear reasons")
	var err error
	if _, err = session.Exec("INSERT INTO golang_driver.sets (id, intset) VALUES (?, ?)",
		4, []int{40, 41, 42}); err == nil {
		t.Error("unexpected: converting from []int to set<int> should not work")
	} else {
		t.Log(err)
	}

}

var (
	setsSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		`CREATE TABLE IF NOT EXISTS golang_driver.sets(id int PRIMARY KEY, tset set<timeuuid>, intset set<int>)`,
		`INSERT INTO golang_driver.sets (id, tset, intset) VALUES (1, {now(), now(), 97294c90-a549-11e5-83b1-dfa924dad615, now(), now()}, {1, 2, 3, 4, 5})`,
	}

	setsCleanup = []string{
		"DROP TABLE golang_driver.sets",
	}
)

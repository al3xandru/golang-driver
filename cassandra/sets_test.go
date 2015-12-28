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

	rows, err := session.Execute("SELECT s FROM golang_driver.sets")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("There should be at least 1 row.")
	}
	var set map[cassandra.UUID]bool

	if err := rows.Scan(&set); err != nil {
		t.Error(err)
	}
	if len(set) != 5 {
		t.Errorf("s set<timeuuid> should have 10 elements; actual %d (%v)",
			len(set), set)
	}
	uuid, _ := cassandra.ParseUUID("97294c90-a549-11e5-83b1-dfa924dad615")
	if !set[uuid] {
		t.Errorf("uuid %s should be in the set %v", uuid.String(), set)
	}
}

var (
	setsSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		`CREATE TABLE IF NOT EXISTS golang_driver.sets(id int PRIMARY KEY, s set<timeuuid>)`,
		`INSERT INTO golang_driver.sets (id, s) VALUES (1, {now(), now(), 97294c90-a549-11e5-83b1-dfa924dad615, now(), now()})`,
	}

	setsCleanup = []string{
		"DROP TABLE golang_driver.sets",
	}
)

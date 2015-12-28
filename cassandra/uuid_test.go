package cassandra_test

import (
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"testing"
)

const (
	timeuuid_ = "9ecc5dd0-a548-11e5-83b1-dfa924dad615"
	uuid_     = "f0d07136-62f9-4d18-a6ce-cd5f4beb4348"
)

func TestUuids(t *testing.T) {
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(uuidSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manually golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(uuidCleanup)

	if err := testInsert(session); err != nil {
		t.Error(err)
	}
	testSelect(t, session)
	// t.Fail()
}

func testInsert(session *cassandra.Session) error {
	// this is just for test purposes
	// it is recommended to either use a real UUID generator
	// or the functions available in Cassandra
	timeUuid, _ := cassandra.ParseUUID(timeuuid_)
	uuid, _ := cassandra.ParseUUID(uuid_)
	_, err := session.Execute("INSERT INTO golang_driver.uuids (id, u, t) VALUES (?, ?, ?)",
		timeUuid, uuid, "second")

	return err
}

func testSelect(t *testing.T, session *cassandra.Session) {
	rows, err := session.Execute("SELECT id, u, t FROM golang_driver.uuids")
	if err != nil {
		t.Fatal(err)
	}
	var uuid, timeuuid cassandra.UUID
	var txt string
	for rows.Next() {
		if err := rows.Scan(&timeuuid, &uuid, &txt); err != nil {
			t.Error(err)
			continue
		}
		if timeuuid.Version() != 1 {
			t.Errorf("timeuuid should be version 1 != %d", timeuuid.Version())
		}
		if uuid.Version() != 4 {
			t.Errorf("uuid should be version 4 != %d", uuid.Version())
		}
		if txt == "second" {
			if timeuuid.String() != timeuuid_ {
				t.Errorf("%s != %s", timeuuid_, timeuuid.String())
			}
			if uuid.String() != uuid_ {
				t.Errorf("%s != %s", uuid_, uuid.String())
			}
		}
	}
}

var (
	uuidSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		"CREATE TABLE IF NOT EXISTS golang_driver.uuids (id timeuuid PRIMARY KEY, u uuid, t text)",
		"INSERT INTO golang_driver.uuids (id, u, t) VALUES (now(), uuid(), 'first')",
	}

	uuidCleanup = []string{
		"DROP TABLE golang_driver.uuids",
	}
)

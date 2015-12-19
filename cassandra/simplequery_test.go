package cassandra_test

import (
	"fmt"
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"testing"
)

func TestSimpleQueries(t *testing.T) {
	session := test.GetSession()
	defer test.Shutdown()

	executeSimpleQuery(session, t)
	executeSimpleQueryWithParams(session, t)
	// t.Fail()
}

func executeSimpleQuery(session *cassandra.Session, t *testing.T) {
	rows, err := session.Execute("select keyspace_name from system.schema_keyspaces")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()

	fmt.Printf("Keyspaces:\n")
	test.IterateRows(rows, t)
}

func executeSimpleQueryWithParams(session *cassandra.Session, t *testing.T) {
	rows, err := session.Execute("select columnfamily_name from system.schema_columnfamilies where keyspace_name = ?",
		"system")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()

	fmt.Printf("Tables:\n")
	test.IterateRows(rows, t)
}

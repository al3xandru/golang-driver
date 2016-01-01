package cassandra_test

import (
	"fmt"
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"testing"
)

func TestPreparedStatement(t *testing.T) {
	session := test.GetSession()
	defer test.Shutdown()

	pstmt, err := session.Prepare("select columnfamily_name from system.schema_columnfamilies where keyspace_name = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer pstmt.Close()

	keyspaces := []string{"system", "system_auth"}
	for _, k := range keyspaces {
		executePreparedStatement(session, pstmt, k, t)
	}
}

func executePreparedStatement(session *cassandra.Session,
	pstmt *cassandra.PreparedStatement,
	param string,
	t *testing.T) {

	rows, err := pstmt.Exec(param)
	if err != nil {
		t.Error(err)
		return
	}

	defer rows.Close()

	fmt.Printf("Tables in keyspace %s:\n", param)
	test.IterateRows(rows, t)
}

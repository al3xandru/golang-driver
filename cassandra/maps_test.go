package cassandra_test

import (
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"testing"
	"time"
)

func TestMaps(t *testing.T) {
	// t.Skipf("Collections do not work yet")
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(mapSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manuallly golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(mapCleanup)

	rows, err := session.Exec("SELECT m FROM golang_driver.maps")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("There should be at least 1 row.")
	}

	var vals map[cassandra.Timestamp]float32

	if err := rows.Scan(&vals); err != nil {
		t.Error(err)
	}
	if len(vals) != 4 {
		t.Errorf("m map<timestamp, float> should have 4 elements; actual %d (%v)",
			len(vals), vals)
	}

	timestamp := time.Unix(1451213010, 0)
	timestampKey := cassandra.NewTimestamp(timestamp)
	if vals[*timestampKey] != 99.99 {
		t.Errorf("m[141213010] 99.99 != %.2f", vals[*timestampKey])
	}
}

var (
	mapSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		`CREATE TABLE IF NOT EXISTS golang_driver.maps(id int PRIMARY KEY, m map<timestamp, float>)`,
		`INSERT INTO golang_driver.maps (id, m) VALUES (1, 
		{1451299450: 101.101, 1451213010: 99.99, 1451126570: 88.88, 1451040130: 42.42})`,
	}

	mapCleanup = []string{
		"DROP TABLE golang_driver.maps",
	}
)

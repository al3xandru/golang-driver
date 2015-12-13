package main

import (
	"fmt"
	"golang-driver/cassandra"
	"time"
)

func main() {
	cluster := cassandra.NewCluster("127.0.0.1")
	//cluster.SetContactPoints("cassandra")
	defer cluster.Close()

	session, err := cluster.Connect()
	if err != nil {
		fmt.Printf("ERROR connecting: %s\r\n", err.Error())
		return
	}
	defer session.Close()
	fmt.Printf("CONNECTED\r\n")
	time.Sleep(5 * time.Second)

	// result, err := session.Execute("select keyspace_name from system.schema_keyspaces")
	ExampleTimeTypes(session)
	session.Execute("select * from system.schema_keyspaces where keyspaces_name = ?", "test")
	fmt.Printf("DONE.\r\n")
}

// CREATE TABLE IF NOT EXISTS typestime (
//		id int PRIMARY KEY,
//		d date,
//		t time,
//		ts timestamp
// );
func ExampleTimeTypes(session *cassandra.Session) {
	d := cassandra.NewDate(1920, 8, 23)
	fmt.Printf("Cassandra Date: %s (%d)\n", d.String(), d.Days)

	result, err := session.Execute("select d, t, ts  from golang.typestime")
	if err != nil {
		fmt.Printf("ERROR Execute: %s\r\n", err.Error())
		return
	}
	defer result.Close()

	fmt.Printf("Clusters:\r\n")
	for result.Next() {
		var d cassandra.Date
		var t cassandra.Time
		var ts cassandra.Timestamp

		if err := result.Scan(&d, &t, &ts); err != nil {
			fmt.Printf("Row error: %s\n", err.Error())
			continue
		}
		fmt.Printf("Date: %s, Time: %s, Timestamp: %d\n", d.String(), t.Duration(), ts)
	}
}

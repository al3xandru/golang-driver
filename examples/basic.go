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

	ExampleSimpleQuery(session)
	ExampleParameterizedQuery(session)
	ExampleTimeTypes(session)
	fmt.Printf("DONE.\r\n")
}

func ExampleSimpleQuery(session *cassandra.Session) {
	result, err := session.Execute("select keyspace_name from system.schema_keyspaces")
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}

	fmt.Printf("Keyspaces:\n")
	for result.Next() {
		var ks string
		if err := result.Scan(&ks); err != nil {
			fmt.Printf("Row error: %s\n", err.Error())
			continue
		}
		fmt.Printf("%s\n", ks)
	}
}

func ExampleParameterizedQuery(session *cassandra.Session) {
	result, err := session.Execute("select * from system.schema_keyspaces where keyspace_name = ?", "test")
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}

	fmt.Printf("Keyspaces:\n")
	for result.Next() {
		var ks string
		if err := result.Scan(&ks); err != nil {
			fmt.Printf("Row error: %s\n", err.Error())
			continue
		}
		fmt.Printf("%s\n", ks)
	}
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

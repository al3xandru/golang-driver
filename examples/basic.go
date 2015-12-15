package main

import (
	"fmt"
	"golang-driver/cassandra"
	"net"
	"time"
)

func main() {
	cluster := cassandra.NewCluster("127.0.0.1")
	defer cluster.Close()

	session, err := cluster.Connect()
	if err != nil {
		fmt.Printf("ERROR connecting: %s\r\n", err.Error())
		return
	}
	defer session.Close()
	fmt.Printf("CONNECTED\r\n")
	time.Sleep(3 * time.Second)

	ExampleSimpleQuery(session)
	ExampleParameterizedQuery(session)
	ExamplePreparedStatement(session)
	ExampleUUIDTypes(session)
	ExampleTimeTypes(session)
	ExampleByteTypes(session)
	fmt.Printf("DONE.\r\n")
}

func ExampleSimpleQuery(session *cassandra.Session) {
	fmt.Println("Simple query")
	result, err := session.Execute("select keyspace_name from system.schema_keyspaces")
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}
	defer result.Close()

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
	fmt.Println("Parameterized query")
	result, err := session.Execute("select * from system.schema_keyspaces where keyspace_name = ?", "test")
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}
	defer result.Close()

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

func ExamplePreparedStatement(session *cassandra.Session) {
	fmt.Println("Prepared statement")
	pStmt, err := session.Prepare("select columnfamily_name from system.schema_columnfamilies where keyspace_name = ?")
	if err != nil {
		fmt.Printf("Prepared statement error: %s\n", err.Error())
		return
	}
	defer pStmt.Close()

	keyspaces := []string{"system", "system_auth"}
	for _, k := range keyspaces {
		stmt, err := pStmt.Bind(k)
		if err != nil {
			fmt.Printf("Binding error: %s\n", err.Error())
			continue
		}
		defer stmt.Close()

		results, err := session.Exec(stmt)
		if err != nil {
			fmt.Printf("Exec error: %s\n", err.Error())
			continue
		}
		defer results.Close()

		fmt.Printf("Tables in keyspace %s:\n", k)
		for results.Next() {
			var tableName string
			if err := results.Scan(&tableName); err != nil {
				fmt.Printf("Row error: %s\n", err.Error())
				continue
			}
			fmt.Printf("\t%s\n", tableName)
		}
	}
}

func ExampleUUIDTypes(session *cassandra.Session) {
	result, err := session.Execute("SELECT t, u FROM golang.typesuuid")
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}
	fmt.Printf("UUIDs results:\n")
	for result.Next() {
		var timeUuid cassandra.UUID
		var uuid cassandra.UUID
		if err := result.Scan(&timeUuid, &uuid); err != nil {
			fmt.Printf("Row error: %s\n", err.Error())
			continue
		}
		fmt.Printf("TimeUUID: %s (Version: %d)\n", timeUuid.String(), timeUuid.Version())
		fmt.Printf("UUID    : %s (Version: %d)\n", uuid.String(), uuid.Version())
	}
}

// CREATE TABLE IF NOT EXISTS typesb (
// 	id int PRIMARY KEY,
// 	b blob,
// 	i inet
// );
func ExampleByteTypes(session *cassandra.Session) {
	result, err := session.Execute("select b, i from golang.typesb")
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}

	fmt.Printf("Byte type results:\n")
	for result.Next() {
		var blob []byte
		var inet net.IP
		if err := result.Scan(&blob, &inet); err != nil {
			fmt.Printf("Row error: %s\n", err.Error())
			continue
		}
		fmt.Printf("Blob: %v\n", blob)
		fmt.Printf("IP  : %s (%v)\n", inet.String(), inet)
	}

	session.Execute("insert into golang.typesb (id, b, i) values (?, ?, ?)",
		int32(3), []byte("cafe"), net.ParseIP("4.4.4.4"))
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

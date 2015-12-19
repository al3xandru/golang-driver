package cassandra_test

import (
	"fmt"
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"net"
	"testing"
)

func TestUuids(t *testing.T) {
	t.Skip("work in progress")
	session := test.GetSession()
	defer test.Shutdown()

	if err := setup(session); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manually golang_driver keyspace")
		t.Fatal(err)
	}
	defer tearDown(session)
	t.Error("just to see what's going on")
}

func insert(session *cassandra.Session) {
	// this is just for test purposes
	// it is recommended to either use a real UUID generator
	// or the functions available in Cassandra
	timeUuid, _ := cassandra.ParseUUID("9ecc5dd0-a548-11e5-83b1-dfa924dad615")
	uuid, _ := cassandra.ParseUUID("f0d07136-62f9-4d18-a6ce-cd5f4beb4348")
	_, err := session.Execute("INSERT INTO golang_driver (id, u, t) VALUES (?, ?, ?)",
		timeUuid, uuid, "second")
	if err != nil {
		fmt.Printf("Insert failed: %s\n", err.Error())
	}
}

var setupStmts = []string{
	"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
	"CREATE TABLE golang_driver.uuids (id timeuuid PRIMARY KEY, u uuid, t text)",
	"INSERT INTO golang_driver.uuids (id, u, txt) VALUES (now(), uuid(), 'first')",
}

var closeStmts = []string{
	"DROP KEYSPACE golang_driver",
}

func setup(session *cassandra.Session) (err error) {
	for _, stmt := range setupStmts {
		fmt.Println(stmt)
		if _, err = session.Execute(stmt); err != nil {
			return
		}
	}
	return
}

func tearDown(session *cassandra.Session) (err error) {
	for _, stmt := range closeStmts {
		if _, err = session.Execute(stmt); err != nil {
			return
		}
	}
	return
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

package main

import (
	"fmt"
	"golang-driver/cassandra"
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

	rows, err := session.Execute("select keyspace_name from system.schema_keyspaces")
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}
	defer rows.Close()

	fmt.Printf("Keyspaces:\n")
	for rows.Next() {
		var ks string
		if err := rows.Scan(&ks); err != nil {
			fmt.Printf("Row error: %s\n", err.Error())
			continue
		}
		fmt.Printf("%s\n", ks)
	}
	fmt.Printf("DONE.\r\n")
}

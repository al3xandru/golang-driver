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

	result, err := session.Execute("select keyspace_name from system.schema_keyspaces")
	if err != nil {
		fmt.Printf("ERROR Execute: %s\r\n", err.Error())
		return
	}
	defer result.Close()

	fmt.Printf("Clusters:\r\n")
	for result.Next() {
		var clusterName string
		if err := result.Scan(&clusterName); err != nil {
			fmt.Printf("Row error: %s\n", err.Error())
			continue
		}
		fmt.Printf("%s\n", clusterName)
	}
	session.Execute("select * from system.schema_keyspaces where keyspaces_name = ?", "test")
/*
	sessfuture := cluster.SessionConnect(session)
	sessfuture.Wait()
	defer sessfuture.Finalize()

	statement := cassandra.NewStatement("select cluster_name from system.local;", 0)
	defer statement.Finalize()

	stmtfuture := session.Execute(statement)
	stmtfuture.Wait()
	defer stmtfuture.Finalize()

	result := stmtfuture.Result()
*/
	fmt.Printf("DONE.\r\n")
}

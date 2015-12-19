package cassandra_test

import (
	"fmt"
	"golang-driver/cassandra"
)

func ExampleClusterConnect() {
	connectionPoints := []string{"127.0.0.1"}
	cluster := cassandra.NewCluster(connectionPoints...)
	defer cluster.Close()

	session, err := cluster.Connect()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer session.Close()
	fmt.Println("Connected")
	// Output:
	// Connected
}

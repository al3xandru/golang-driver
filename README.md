golang-driver
=============

A Go wrapper for the DataStax/Cassandra [C/C++ driver](https://github.com/datastax/cpp-driver)

This is my Go and C learning experience so treat it as an experiment. It builds
on the work of Matt Stump's
[golang-driver](https://github.com/mstump/golang-driver) and uses the version
2.2 of the [DataStax C/C++ driver for Cassandra](https://github.com/datastax/cpp-driver).

### Build

1. [Build](http://datastax.github.io/cpp-driver/topics/building/) and install the DataStax [C/C++ driver](https://github.com/datastax/cpp-driver)
2. Install `go get github.com/al3xandru/golang-driver/cassandra`
3. Run the example `go run $GOPATH/src/github.com/al3xandru/golang-driver/examples/basic.go`

### Example Usage

```go
package main

import (
	"fmt"
	"golang-driver/cassandra"
)

func main() {
	cluster := cassandra.NewCluster("127.0.0.1", "127.0.0.2")
	defer cluster.Close()

	session, err := cluster.Connect()
	if err != nil {
		fmt.Printf("Error connecting: %s\n", err.Error())
		return
	}
	defer session.Close()

	result, err := session.Execute("select keyspace_name from system.schema_keyspaces")
	if err != nil {
		fmt.Printf("Error executing: %s\n", err.Error())
		return
	}
	defer result.Close()

	fmt.Printf("Keyspaces:\n")
	for result.Next() {
		var keyspace string
		if err := result.Scan(&keyspace); err != nil {
			fmt.Printf("Row error: %s\n", err.Error())
			continue
		}
		fmt.Printf("%s\n", keyspace)
	}
}
```

### To do

* [X] Cassandra `tinyint` (`int8`) and `smallint` (`int16`)
* [X] Date time types
* [X] Binding values to statements
* [ ] Prepared statements
* [ ] Missing C* types: `inet`, `decimal`, `binary`, `uuid`, `timeuuid`
* [ ] Async API
* [ ] Advanced cluster configuration
* [ ] Support for collections
* [ ] Support for tuples
* [ ] Support for UDTs

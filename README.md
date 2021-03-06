golang-driver
=============

A Go wrapper for the super performant and tunable [DataStax C/C++ driver for Apache Cassandra](https://github.com/datastax/cpp-driver).

Originally inspired by Matt Stump's [golang-driver](https://github.com/mstump/golang-driver), 
this project evolved and added a lot of new features (e.g. support for all
Cassandra data types, a simpler API that supports both simple and prepared
statements in both sync and async flavors, cluster configuration options, etc.)

_While this far this project has been my experiment in learning Go and interacting with
the [C/C++ driver for Cassandra](https://github.com/datastax/cpp-driver), I
think that the next stage of this project would be to benefit from the expertise
of more experienced Go programmers._

_**If you are interested in continuing to develop this project, please drop me 
an email** (alex at mypopescu dot com)._


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

	result, err := session.Exec("select keyspace_name from system.schema_keyspaces")
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

See the tests in the main package for more examples.

### What is supposed to work

1. Connecting to a cluster (sort of minimal expectation) with support for some
   of the configuration options (not all options provided by the C/C++ driver
   are available)
2. A range of basic Cassandra types, including the new ones introduced in
   version 2.2 (tinyint, smallint, date, time, timestamp). 

   * There are a couple of missing data types (see [To dos](#to-do)).
   * There's no support yet for UDTs

3. Executing simple statements:

    ```go
    session.Exec("select * from table where pk = ?", pk_value)
    ```

4. Executing simple statements with non-default consistency settings:

    ```go
    session.Query("select * from table where pk = ?", pk_value).
            WithConsistency(ANY).
            Exec()
    ```

5. Executing prepared statements:

    ```go
    pstmt := session.Prepare("select * from table where pk = ?")
    pstmt.SetConsistency(ANY)
    // once prepared you can reuse the prepared statement by
    // binding new sets of parameters
    for key := range keys {
        pstmt.Exec(key)
    }
    ```

6. There are similar functions for executing async statements which return a
   `*cassandra.Future`


#### Go types, driver types, and Cassandra data types

In order to expose all Cassandra types, this driver introduces some custom types
whose usage should be fairly simple and self-explanatory. All of these types and
their API can be found in [types.go](./cassandra/types.go). Here's a short list:

* `Timestamp`: corresponds to the `timestamp` data type and represents the seconds since Epoch 
* `Date`: corresponds to the `date` data type and holds a date without a time
    component
* `Time`: corresponds to the `time` data type and represents a time within a day
* `UUID`: for both `uuid` and `timeuuid`
* `Decimal`: corresponds to the `decimal` data type and represents an arbitrary
    precision decimal number
* `Tuple`: corresponds to the `tuple` data type. 


##### Decimal

This library provides a very basic `cassandra.Decimal` type with just a couple
of functions to allow you to store/retrieve the Cassandra `decimal` type.

In case you need more complete decimal libraries (that provide math operations
on decimals), this is what I could find:

* [decimal library for Go](http://engineering.shopspring.com/2015/03/03/decimal/)
* [*fpd.Decimal](https://github.com/oguzbilgic/fpd)

Converting from one of these to the `cassandra.Decimal` for storage should work
without any problems.

##### Sets

Go doesn't have a `Set`-like type which makes it very hard to create an
automatic mapping to a Cassandra `set` column with non-prepared statements. If
prepared statements are used, then metadata about the target column is available
and conversions from `slices`, `arrays`, and `map[type]bool` can be made.

This library offers a `cassandra.Set()` function that can wrap a slice, array,
or map to make it aware that the target column is `set`. 

If you are using prepared statements, you won't need this function.


## Credits

* [go.uuid](https://github.com/satori/go.uuid) and [uuid](https://github.com/pborman/uuid) for inspiration on dealing with UUIDs
* [gocql](https://github.com/gocql/gocql) for teaching me a lot about Go
    `reflect`
* countless people on #go-nuts and Twitter

### To do

* [X] Support for Cassandra 2.2 `tinyint` (`int8`) and `smallint` (`int16`) ([CASSANDRA-8951](https://issues.apache.org/jira/browse/CASSANDRA-8951)
* [X] Support for Cassandra 2.2 date/time types ([CASSANDRA-7523](https://issues.apache.org/jira/browse/CASSANDRA-7523))
* [X] Binding values to statements
* [X] Read/Write Cassandra `blob` (`[]byte`) and `inet` (`net.IP`)
* [X] Prepared statements
* [X] Basic support for Cassandra `uuid`, `timeuuid` using `cassandra.UUID`
    struct
* [X] Advanced cluster configuration
* [X] Async API
* [X] Support for collections 
* [X] Missing C* types: `decimal`, `varint`
* [X] Support for tuples (at least those using non-collections)
* [ ] Support for UDTs
* [ ] Named parameters
* [ ] Unset (v4) vs null parameters
* [ ] Batch statements


Copyright 2015-2016 Alex Popescu


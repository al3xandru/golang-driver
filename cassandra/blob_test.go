package cassandra_test

import (
	"fmt"
	"golang-driver/cassandra"
	"net"
)

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

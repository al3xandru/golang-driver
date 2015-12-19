package test

import (
	"fmt"
	"golang-driver/cassandra"
	"sync"
	"testing"
)

var DB struct {
	sync.Mutex
	cluster *cassandra.Cluster
	session *cassandra.Session
	n       int
}

func initialize(contactPoints ...string) {
	fmt.Println("...initiatilizing DB connection")
	if len(contactPoints) == 0 {
		contactPoints = []string{"127.0.0.1"}
	}
	DB.cluster = cassandra.NewCluster(contactPoints...)
	if session, err := DB.cluster.Connect(); err != nil {
		panic(err.Error())
	} else {
		DB.session = session
	}
}

func GetSession(contactPoints ...string) *cassandra.Session {
	DB.Lock()
	fmt.Printf("DB entry: %d\n", DB.n)
	if DB.n == 0 {
		initialize(contactPoints...)
	}
	DB.n++
	DB.Unlock()
	return DB.session
}

func Shutdown() {
	DB.Lock()
	DB.n--
	fmt.Printf("DB exit : %d\n", DB.n)
	if DB.n == 0 {
		DB.session.Close()
		DB.cluster.Close()
		fmt.Println("... disconnected")
	}
	DB.Unlock()
}

func IterateRows(rows *cassandra.Rows, t *testing.T) {
	for rows.Next() {
		var ks string
		if err := rows.Scan(&ks); err != nil {
			t.Error(err)
			continue
		}
		fmt.Printf("\t%s\n", ks)
	}
}

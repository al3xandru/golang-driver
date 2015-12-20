package test

import (
	"fmt"
	"golang-driver/cassandra"
	"sync"
	"testing"
)

var DB struct {
	sync.Mutex
	session *cassandra.Session
	n       int
}

func initialize(contactPoints ...string) {
	fmt.Println("...initiatilizing DB connection")
	if len(contactPoints) == 0 {
		contactPoints = []string{"127.0.0.1"}
	}
	cluster := cassandra.NewCluster(contactPoints...)
	if session, err := cluster.Connect(); err != nil {
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
		cluster := DB.session.Cluster
		DB.session.Close()
		cluster.Close()
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

func Setup(statements []string) (err error) {
	for _, stmt := range statements {
		fmt.Println(stmt)
		if _, err = DB.session.Execute(stmt); err != nil {
			return
		}
	}
	return
}

func TearDown(statements []string) {
	for _, stmt := range statements {
		if _, err := DB.session.Execute(stmt); err != nil {
			fmt.Printf("%s executing closing statement '%s'\n", err.Error(), stmt)
		}
	}
}

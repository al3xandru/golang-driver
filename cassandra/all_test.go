package cassandra_test

import (
	"golang-driver/cassandra/test"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	test.GetSession("127.0.0.1")
	r := m.Run()
	test.Shutdown()
	os.Exit(r)
}

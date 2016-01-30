package cassandra_test

import (
	"golang-driver/cassandra"
	"testing"
)

func TestCassTypeName(t *testing.T) {
	tupleType := cassandra.CTuple.Specialize(cassandra.CInt,
		cassandra.CText,
		cassandra.CList.Specialize(cassandra.CTimeuuid))
	var expected = "tuple<int, text, list<timeuuid>>"
	if expected != tupleType.String() {
		t.Errorf("%s != %s", tupleType.String(), expected)
	}
}

package cassandra_test

import (
	"golang-driver/cassandra"
	"testing"
)

func TestCassTypeName(t *testing.T) {
	tupleType := cassandra.CASS_TUPLE.Subtype(cassandra.CASS_INT,
		cassandra.CASS_TEXT,
		cassandra.CASS_LIST.Subtype(cassandra.CASS_TIMEUUID))
	if "tuple<int, text, list<timeuuid>>" != tupleType.Name() {
		t.Errorf("%s not expected", tupleType.Name())
	}
}

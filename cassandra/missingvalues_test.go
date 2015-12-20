package cassandra_test

import (
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"net"
	"testing"
)

func TestMissingValues(t *testing.T) {
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(alltypesSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manually golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(alltypesCleanup)
	rows, err := session.Execute("SELECT a, txt, vchr, bol, bigi, ii, smalli, tinyi, dbl, flt, ip, tuid, uid, blb FROM golang_driver.alltypes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var asc, txt, vchar string
	var bo bool
	var dbl float64
	var flt float32
	var bi int64
	var ii int32
	var si int16
	var ti int8
	var bl []byte
	var tu cassandra.UUID
	var uu cassandra.UUID
	var ip net.IP

	var empty [16]byte

	if rows.Next() {
		if err := rows.Scan(&asc, &txt, &vchar, &bo, &bi, &ii, &si, &ti, &dbl, &flt, &ip, &tu, &uu, &bl); err != nil {
			t.Fatal(err)
		}
		if asc != "" {
			t.Errorf("default ascii should be \"\" != \"%s\"", asc)
		}
		if txt != "" {
			t.Errorf("default text should be \"\" != \"%s\"", txt)
		}
		if vchar != "" {
			t.Errorf("default varchar should be \"\" != \"%s\"", vchar)
		}
		// int types
		if bi != 0 {
			t.Errorf("default bigint should be 0 != %d", bi)
		}
		if ii != 0 {
			t.Errorf("default int should be 0 != %d", ii)
		}
		if si != 0 {
			t.Errorf("default smallint should be 0 != %d", si)
		}
		if ti != 0 {
			t.Errorf("default tinyint should be 0 != %d", ti)
		}
		if bo {
			t.Errorf("default boolean should be false != %t", bo)
		}
		// float, double
		if dbl != 0.0 {
			t.Errorf("default double should be 0.0 != %f", dbl)
		}
		if flt != 0.0 {
			t.Errorf("default float should be 0.0 != %f", flt)
		}

		if len(bl) != 0 {
			t.Errorf("default blob should be [] != %v", bl)
		}
		// UUIDs
		if tu != empty {
			t.Errorf("default timeuuid should be zeroed != %s", tu.String())
		}
		if uu != empty {
			t.Errorf("default uuid should be zeroed != %s", uu.String())
		}
		// IP
		if ip != nil || len(ip) != 0 {
			t.Errorf("default inet should be empty != %s", ip.String())
		}
		// time types
		// if d != nil t {
		// 	t.Errorf("default date should be 0 (%s)", d.String())
		// }
		// if tm.Nanos != 0 {
		// 	t.Errorf("default time should be 0 != %d (%s)", tm.Nanos, tm.Duration())
		// }
		// if ts != 0 {
		// 	t.Errorf("default timestamp should be 0 != %d", ts)
		// }
	} else {
		t.Fatal("There must be 1 row in golang_driver.alltypes")
	}
}

func testIntResult(session *cassandra.Session, t *testing.T) {
	// if a result is read in int or uint it should error
	// C* `int` maps to `int32`
}

var (
	alltypesSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		`CREATE TABLE IF NOT EXISTS golang_driver.alltypes (id int PRIMARY KEY, 
		a ascii, bigi bigint, blb blob, bol boolean, td date,
		dbl double, flt float, ip inet, ii int, smalli smallint,
		txt text, tm time, ts timestamp, tuid timeuuid, tinyi tinyint,
		uid uuid, vchr varchar, vi varint)`,
		"INSERT INTO golang_driver.alltypes (id) VALUES (1)",
	}

	alltypesCleanup = []string{
		"DROP TABLE golang_driver.alltypes",
	}
)

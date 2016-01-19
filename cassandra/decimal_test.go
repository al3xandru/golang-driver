package cassandra_test

import (
	"fmt"
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"math/big"
	"testing"
)

func TestNewDecimal(t *testing.T) {
	d := cassandra.NewDecimal(131312312323423423, 11)
	if d.String() != "1313123.12323423423" {
		t.Errorf("decimal %s != 1313123.12323423423", d)
	}
}

// func TestNewDecimalFromFloat(t *testing.T) {
// 	dec := cassandra.NewDecimalFromFloat(1313123.12323423423)
// 	if dec.String() != "1313123.12323423423" {
// 		t.Errorf("%s != %s", dec, "1313123.12323423423")
// 	}
// }

func TestParseDecimal(t *testing.T) {
	if _, err := cassandra.ParseDecimal(""); err == nil {
		t.Errorf("empty string is not a valid Decimal")
	} else {
		t.Log(err)
	}
	if _, err := cassandra.ParseDecimal("123.456.789"); err == nil {
		t.Errorf("123.456.789 is not a valid Decimal")
	} else {
		t.Log(err)
	}
	if _, err := cassandra.ParseDecimal("12vv34.56789"); err == nil {
		t.Errorf("12vv34.5679 is not a valid Decimal")
	} else {
		t.Log(err)
	}
	d, err := cassandra.ParseDecimal("-12345.6789")
	if err != nil {
		t.Error(err)
	}
	if d.Scale != 4 {
		t.Errorf("scale %d != 4", d.Scale)
	}
	if d.String() != "-12345.6789" {
		t.Errorf("decimal %s != -12345.6789", d)
	}
	d, err = cassandra.ParseDecimal("1313123123.234234234234234234123")
	if err != nil {
		t.Error(err)
	}
	if d.Scale != 21 {
		t.Errorf("scale %d != 21", d.Scale)
	}
	if d.String() != "1313123123.234234234234234234123" {
		t.Errorf("decimal %s != 1313123123.234234234234234234123", d)
	}
	// t.Error("log")
}

func TestDecimalAndVarint(t *testing.T) {
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(bignumbersSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manually golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(bignumbersCleanup)

	testInsertDecimalVarintUsingStatement(t, session)
	testInsertDecimalVarintUsingPreparedStatement(t, session)
	testSelectDecimalVarint(t, session)
	t.Error("log")
}

func testSelectDecimalVarint(t *testing.T, session *cassandra.Session) {
	rows, err := session.Exec("SELECT dec, vint from golang_driver.bignumbers")
	if err != nil {
		t.Error(err)
		return
	}

	// if !rows.Next() {
	// 	t.Errorf("expecting at least 1 row")
	// 	return
	// }
	var dec cassandra.Decimal
	var vint big.Int

	for rows.Next() {
		if err := rows.Scan(&dec, &vint); err != nil {
			t.Error(err)
			return
		}
		fmt.Printf("decimal: %s\n", dec.String())
		fmt.Printf("varint : %s\n", vint.String())
	}
	// if dec.Scale != 20 {
	// 	t.Errorf("decimal scale %s != 20", dec.String())
	// }
	// if dec.String() != "123456789.12345678901234567890" {
	// 	t.Errorf("decimal %s != 123456789.12345678901234567890", dec.String())
	// }
	// if vint.String() != "12345678901234567890" {
	// 	t.Errorf("varint %s != 12345678901234567890", vint.String())
	// }
}

func testInsertDecimalVarintUsingStatement(t *testing.T, session *cassandra.Session) {
	dec := createDecimal("123456789.12345678901234567890", t)
	vint := createVarint("12345678901234567890", t)
	if _, err := session.Exec("INSERT INTO golang_driver.bignumbers (id, dec, vint) VALUES (?, ?, ?)",
		100, dec, vint); err != nil {
		t.Error(err)
	}

	dec = createDecimal("-98765.43210", t)
	vint = createVarint("-9876543210987654321", t)
	if _, err := session.Exec("INSERT INTO golang_driver.bignumbers (id, dec, vint) VALUES (?, ?, ?)",
		101, dec, vint); err != nil {
		t.Error(err)
	}
}

func testInsertDecimalVarintUsingPreparedStatement(t *testing.T, session *cassandra.Session) {
	pstmt, err := session.Prepare("INSERT INTO golang_driver.bignumbers (id, dec, vint) VALUES (?, ?, ?)")
	if err != nil {
		t.Error(err)
		return
	}

	dec := createDecimal("123456789.12345678901234567890", t)
	vint := createVarint("12345678901234567890", t)
	if _, err = pstmt.Exec(1000, dec, vint); err != nil {
		t.Error(err)
	}

	dec = createDecimal("-98765.43210", t)
	vint = createVarint("-9876543210987654321", t)
	if _, err = pstmt.Exec(1001, dec, vint); err != nil {
		t.Error(err)
	}
}

func createDecimal(v string, t *testing.T) *cassandra.Decimal {
	dec, err := cassandra.ParseDecimal(v)
	if err != nil {
		t.Fatal(err)
	}
	return dec
}

func createVarint(v string, t *testing.T) *big.Int {
	vint := big.NewInt(0)
	vint, ok := vint.SetString(v, 10)
	if !ok {
		t.Fatalf("cannot create math/big.Int %s", v)
	}
	return vint
}

var (
	bignumbersSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		`CREATE TABLE IF NOT EXISTS golang_driver.bignumbers (id int PRIMARY KEY, dec decimal, vint varint)`,
		`INSERT INTO golang_driver.bignumbers (id, dec, vint) VALUES (1, 123456789.12345678901234567890, 12345678901234567890)`,
		`INSERT INTO golang_driver.bignumbers (id, dec, vint) VALUES (2, -123456789.12345678901234567890, -12345678901234567890)`,
	}

	bignumbersCleanup = []string{
	// "DROP TABLE golang_driver.bignumbers",
	}
)

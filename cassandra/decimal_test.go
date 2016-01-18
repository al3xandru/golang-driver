package cassandra_test

import (
	"golang-driver/cassandra"
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

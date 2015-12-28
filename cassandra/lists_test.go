package cassandra_test

import (
	"golang-driver/cassandra"
	"golang-driver/cassandra/test"
	"testing"
)

func TestLists(t *testing.T) {
	// t.Skipf("Collections do not work yet")
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(colSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manuallly golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(colCleanup)

	rows, err := session.Execute("SELECT intlist, textlist, datelist FROM golang_driver.listtypes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("There should be at least 1 row.")
	}
	var intLst []int
	var strLst []string
	var dateLst []cassandra.Date

	if err := rows.Scan(&intLst, &strLst, &dateLst); err != nil {
		t.Error(err)
	}
	if len(intLst) != 10 {
		t.Errorf("intlist list<int> should have 10 elements; actual %d (%v)",
			len(intLst), intLst)
	}
	if intLst[3] != 44 {
		t.Errorf("intlist[3] 44 != %d", intLst[3])
	}
	if len(strLst) != 8 {
		t.Errorf("strlist (list<text>) should have 8 elements; actual %d (%v)",
			len(strLst), strLst)
	}
	if strLst[6] != "J.J. Abrams" {
		t.Errorf("strlist[6] \"J.J. Abrams\" != %s", strLst[6])
	}
	if len(dateLst) != 8 {
		t.Errorf("datelist (list<date>) should have 8 elements; actual %d (%v)",
			len(dateLst), dateLst)
	}
	if dateLst[6].String() != "2015-12-18" {
		t.Errorf("datelist[6] \"2015-12-18\" != %s", dateLst[6].String())
	}
}

var (
	colSetup = []string{
		"CREATE KEYSPACE IF NOT EXISTS golang_driver WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
		`CREATE TABLE IF NOT EXISTS golang_driver.listtypes(id int PRIMARY KEY, 
		intlist list<int>, datelist list<date>, textlist list<text>)`,
		`INSERT INTO golang_driver.listtypes (id, intlist, datelist, textlist) VALUES (1, 
			[11, 22, 33, 44, 55, 66, 77, 88, 99, 10], 
			['1977-5-25', '1980-05-21', '1983-05-25', '1999-05-19', '2002-05-16', '2005-5-19', '2015-12-18', '2017-05-26'],
			['George Lucas', 'Irvin Kershner', 'Richard Marquand', 'George Lucas', 'George Lucas', 'George Lucas', 'J.J. Abrams', 'Rian Johnson'])`,
	}

	colCleanup = []string{
		"DROP TABLE golang_driver.listtypes",
	}
)

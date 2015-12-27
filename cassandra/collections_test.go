package cassandra_test

import (
	"golang-driver/cassandra/test"
	"reflect"
	"testing"
)

func TestSlice(t *testing.T) {
	intSlice := []int{}
	strSlice := []string{}

	iKind := reflect.TypeOf(intSlice)
	t.Errorf("int slice: %s", iKind.String())
	if iKind.Kind() == reflect.Slice {
		t.Logf("I'm good")
		eKind := iKind.Elem()
		t.Logf("\telem type: %s", eKind.String())
	}
	sKind := reflect.TypeOf(strSlice)
	t.Errorf("str slice: %s", sKind.String())
}

func TestMap(t *testing.T) {
	intstrMap := map[int]string{}
	// ssMap := map[string]interface{}{}

	t1 := reflect.TypeOf(intstrMap)
	t.Errorf("1: %s", t1.String())
	if t1.Kind() == reflect.Map {
		t.Logf("key: %s", t1.Key().String())
		t.Logf("val: %s", t1.Elem().String())
	}
}

func TestReflect(t *testing.T) {
	f := func(v interface{}) string {
		switch v := v.(type) {
		case *[]int:
			*v = append(*v, 1)
			return "slice []int"
		case []string:
			v = append(v, "def")
			return "slice []string"
		case *[]string:
			*v = append(*v, "abc")
			return "slice []string"
		case []interface{}:
			return "slice"
		case map[interface{}]interface{}:
			return "map"
		}
		return reflect.TypeOf(v).String()
	}
	var iSlice []int
	var sSlice []string
	var isMap map[int]string
	var ssMap map[string]string
	var siMap map[string]int

	t.Error(f(&iSlice))
	t.Error(iSlice)
	t.Error(f(sSlice))
	t.Error(sSlice)
	t.Error(f(&sSlice))
	t.Error(sSlice)
	t.Error(f(isMap))
	t.Error(f(ssMap))
	t.Error(f(siMap))
}

func TestMapCasts(t *testing.T) {
	f1 := func() interface{} {
		return map[string]int{
			"abc": 1,
			"def": 2,
			"ghi": 3,
		}
	}
	r := f1()
	r = r.(map[string]int)
	t.Error(reflect.TypeOf(r).String())
}
func TestCollections(t *testing.T) {
	// t.Skipf("Collections do not work yet")
	session := test.GetSession()
	defer test.Shutdown()

	if err := test.Setup(colSetup); err != nil {
		t.Log("Unexpected error while setup. You might need to clean up manuallly golang_driver keyspace")
		t.Fatal(err)
	}
	defer test.TearDown(colCleanup)

	rows, err := session.Execute("SELECT intlist, textlist FROM golang_driver.listtypes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("There should be at least 1 row.")
	}
	var intLst []int
	var strLst []string
	// var dateLst []cassandra.Date
	// var txtLst []string
	if err := rows.Scan(&intLst, &strLst); err != nil {
		t.Error(err)
	}
	t.Logf("[]int: %d", intLst)
	t.Logf("[]string: %v", strLst)
	t.Error("see results")
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

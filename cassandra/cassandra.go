package cassandra

// #cgo LDFLAGS: -L/usr/local/lib -lcassandra
// #cgo CFLAGS: -I/usr/local/include
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import "unsafe"

import (
	"errors"
	"fmt"
	"reflect"
)

type Session struct {
	cptr    *C.struct_CassSession_
	Cluster *Cluster
}

func (session *Session) Close() {
	C.cass_session_free(session.cptr)
	session.cptr = nil
	session.Cluster = nil
}

func (session *Session) Exec(query string, args ...interface{}) (*Rows, error) {
	stmt := newSimpleStatement(session, query, len(args))
	defer stmt.Close()

	stmt.bind(args...)
	return stmt.Exec()
}

func (session *Session) Query(query string, args ...interface{}) *Statement {
	stmt := newSimpleStatement(session, query, len(args))
	stmt.Args = args

	return stmt
}

func (session *Session) Prepare(query string) (*PreparedStatement, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	future := async(func() *C.struct_CassFuture_ {
		return C.cass_session_prepare(session.cptr, cQuery)
	})
	defer future.Close()

	if err := future.Error(); err != nil {
		return nil, err
	}
	future.Wait()

	pstmt := new(PreparedStatement)
	pstmt.cptr = C.cass_future_get_prepared(future.cptr)
	pstmt.session = session
	pstmt.consistency = unset
	pstmt.serialConsistency = unset

	return pstmt, nil
}

type Consistency int

const (
	unset Consistency = 1 << iota
	ANY
	ONE
	TWO
	THREE
	QUORUM
	ALL
	LOCAL_QUORUM
	EACH_QUORUM
	// Serial Consistency
	SERIAL
	LOCAL_SERIAL
	LOCAL_ONE
)

func (c Consistency) toC() C.CassConsistency {
	switch c {
	case ANY:
		return C.CassConsistency(C.CASS_CONSISTENCY_ANY)
	case ONE:
		return C.CassConsistency(C.CASS_CONSISTENCY_ONE)
	case TWO:
		return C.CassConsistency(C.CASS_CONSISTENCY_TWO)
	case THREE:
		return C.CassConsistency(C.CASS_CONSISTENCY_THREE)
	case QUORUM:
		return C.CassConsistency(C.CASS_CONSISTENCY_QUORUM)
	case ALL:
		return C.CassConsistency(C.CASS_CONSISTENCY_ALL)
	case LOCAL_QUORUM:
		return C.CassConsistency(C.CASS_CONSISTENCY_LOCAL_QUORUM)
	case EACH_QUORUM:
		return C.CassConsistency(C.CASS_CONSISTENCY_EACH_QUORUM)
	case SERIAL:
		return C.CassConsistency(C.CASS_CONSISTENCY_SERIAL)
	case LOCAL_SERIAL:
		return C.CassConsistency(C.CASS_CONSISTENCY_LOCAL_SERIAL)
	case LOCAL_ONE:
		return C.CassConsistency(C.CASS_CONSISTENCY_LOCAL_ONE)
	}
	return C.CassConsistency(C.CASS_CONSISTENCY_UNKNOWN)
}

type PreparedStatement struct {
	cptr              *C.struct_CassPrepared_
	session           *Session
	consistency       Consistency
	serialConsistency Consistency
	pagingSize        int
}

func (pstmt *PreparedStatement) SetConsistency(c Consistency) {
	pstmt.consistency = c
}

func (pstmt *PreparedStatement) SetSerialConsistency(c Consistency) {
	pstmt.serialConsistency = c
}

func (pstmt *PreparedStatement) Close() {
	C.cass_prepared_free(pstmt.cptr)
	pstmt.cptr = nil
}

func (pstmt *PreparedStatement) Exec(args ...interface{}) (*Rows, error) {
	future := pstmt.ExecAsync(args...)
	defer future.Close()

	if err := future.Error(); err != nil {
		return nil, err
	}

	return future.Result(), nil
}

func (pstmt *PreparedStatement) ExecAsync(args ...interface{}) *Future {
	stmt := newBoundStatement(pstmt)
	stmt.Args = args
	stmt.WithConsistency(pstmt.consistency)
	stmt.WithSerialConsistency(pstmt.serialConsistency)

	defer stmt.Close()

	return stmt.ExecAsync()
}

type Future struct {
	cptr *C.struct_CassFuture_
	err  error
}

func (future *Future) Error() error {
	// shortcircuit if an error has already been set
	if future.err != nil {
		return future.err
	}
	if C.cass_future_error_code(future.cptr) == C.CASS_OK {
		return nil
	}
	var msg *C.char
	var sizet C.size_t

	C.cass_future_error_message(future.cptr, &msg, &sizet)
	return errors.New(C.GoStringN(msg, C.int(sizet)))
}

func (future *Future) Result() *Rows {
	rows := new(Rows)
	rows.cptr = C.cass_future_get_result(future.cptr)
	return rows
}

func (future *Future) Wait() {
	C.cass_future_wait(future.cptr)
}

func (future *Future) WaitTimed(timeout uint64) bool {
	return C.cass_future_wait_timed(future.cptr, C.cass_duration_t(timeout)) == C.cass_true
}

func (future *Future) Close() {
	if future.err != nil {
		return
	}
	C.cass_future_free(future.cptr)
	future.cptr = nil
}

type Rows struct {
	iter *C.struct_CassIterator_
	cptr *C.struct_CassResult_
	err  error
}

func (r *Rows) Err() error {
	return r.err
}

func (rows *Rows) Close() {
	C.cass_result_free(rows.cptr)
	rows.cptr = nil
}

func (rows *Rows) ColumnCount() uint64 {
	return uint64(C.cass_result_column_count(rows.cptr))
}

func (rows *Rows) ColumnName(index int) string {
	var cStr *C.char
	var size C.size_t
	retc := C.cass_result_column_name(rows.cptr, C.size_t(index), &cStr, &size)
	if retc == C.CASS_OK {
		return C.GoStringN(cStr, C.int(size))
	}
	return "UNKNOWN"
}

func (rows *Rows) ColumnType(index int) string {
	return cassTypeName(C.cass_result_column_type(rows.cptr, C.size_t(index)))
}

func (rows *Rows) Next() bool {
	if rows.iter == nil {
		rows.iter = C.cass_iterator_from_result(rows.cptr)
	}
	return C.cass_iterator_next(rows.iter) != 0
}

func (rows *Rows) Scan(args ...interface{}) error {
	if rows.ColumnCount() < uint64(len(args)) {
		return errors.New("invalid argument count")
	}

	row := C.cass_iterator_get_row(rows.iter)

	for i, v := range args {
		value := C.cass_row_get_column(row, C.size_t(i))
		casst := C.cass_result_column_type(rows.cptr, C.size_t(i))

		if _, err := read(value, casst, v); err != nil {
			return newColumnError(rows, i, v, err)
		}
	}

	return nil
}

const (
	CASS_VALUE_TYPE_UNKNOWN   = 0xFFFF
	CASS_VALUE_TYPE_CUSTOM    = 0x0000
	CASS_VALUE_TYPE_ASCII     = 0x0001
	CASS_VALUE_TYPE_BIGINT    = 0x0002
	CASS_VALUE_TYPE_BLOB      = 0x0003
	CASS_VALUE_TYPE_BOOLEAN   = 0x0004
	CASS_VALUE_TYPE_COUNTER   = 0x0005
	CASS_VALUE_TYPE_DECIMAL   = 0x0006
	CASS_VALUE_TYPE_DOUBLE    = 0x0007
	CASS_VALUE_TYPE_FLOAT     = 0x0008
	CASS_VALUE_TYPE_INT       = 0x0009
	CASS_VALUE_TYPE_TEXT      = 0x000A
	CASS_VALUE_TYPE_TIMESTAMP = 0x000B
	CASS_VALUE_TYPE_UUID      = 0x000C
	CASS_VALUE_TYPE_VARCHAR   = 0x000D
	CASS_VALUE_TYPE_VARINT    = 0x000E
	CASS_VALUE_TYPE_TIMEUUID  = 0x000F
	CASS_VALUE_TYPE_INET      = 0x0010
	CASS_VALUE_TYPE_DATE      = 0x0011
	CASS_VALUE_TYPE_TIME      = 0x0012
	CASS_VALUE_TYPE_SMALL_INT = 0x0013
	CASS_VALUE_TYPE_TINY_INT  = 0x0014
	CASS_VALUE_TYPE_LIST      = 0x0020
	CASS_VALUE_TYPE_MAP       = 0x0021
	CASS_VALUE_TYPE_SET       = 0x0022
	CASS_VALUE_TYPE_UDT       = 0x0030
	CASS_VALUE_TYPE_TUPLE     = 0x0031
)

func cassTypeName(kind C.CassValueType) string {
	switch kind {
	case CASS_VALUE_TYPE_ASCII:
		return "ascii"
	case CASS_VALUE_TYPE_BIGINT:
		return "bigint"
	case CASS_VALUE_TYPE_BLOB:
		return "blob"
	case CASS_VALUE_TYPE_BOOLEAN:
		return "boolean"
	case CASS_VALUE_TYPE_COUNTER:
		return "counter"
	case CASS_VALUE_TYPE_DECIMAL:
		return "decimal"
	case CASS_VALUE_TYPE_DOUBLE:
		return "double"
	case CASS_VALUE_TYPE_FLOAT:
		return "float"
	case CASS_VALUE_TYPE_INT:
		return "int"
	case CASS_VALUE_TYPE_TEXT:
		return "text"
	case CASS_VALUE_TYPE_TIMESTAMP:
		return "timestamp"
	case CASS_VALUE_TYPE_UUID:
		return "uuid"
	case CASS_VALUE_TYPE_VARCHAR:
		return "varchar"
	case CASS_VALUE_TYPE_VARINT:
		return "varint"
	case CASS_VALUE_TYPE_TIMEUUID:
		return "timeuuid"
	case CASS_VALUE_TYPE_INET:
		return "inet"
	case CASS_VALUE_TYPE_DATE:
		return "date"
	case CASS_VALUE_TYPE_TIME:
		return "time"
	case CASS_VALUE_TYPE_SMALL_INT:
		return "smallint"
	case CASS_VALUE_TYPE_TINY_INT:
		return "tinyint"
	case CASS_VALUE_TYPE_LIST:
		return "list"
	case CASS_VALUE_TYPE_MAP:
		return "map"
	case CASS_VALUE_TYPE_SET:
		return "set"
	case CASS_VALUE_TYPE_UDT:
		return "udt"
	case CASS_VALUE_TYPE_TUPLE:
		return "tuple"
	case CASS_VALUE_TYPE_CUSTOM:
		return "custom"
	default:
		return "UNKNOWN"
	}
}

func newError(retc C.CassError) error {
	return errors.New(C.GoString(C.cass_error_desc(retc)))
}

func newColumnError(rows *Rows, index int, v interface{}, err error) error {
	columnName := rows.ColumnName(index)
	columnType := rows.ColumnType(index)
	argType := reflect.TypeOf(v).String()
	errMsg := fmt.Sprintf("%s (arg %d, type: %s, column: %s, type: %s)",
		err.Error(),
		index,
		argType,
		columnName,
		columnType)
	return errors.New(errMsg)
}

type statement struct {
	cptr *C.struct_CassStatement_
}

func async(f func() *C.struct_CassFuture_) *Future {
	ptrFuture := f()
	return &Future{cptr: ptrFuture}
}

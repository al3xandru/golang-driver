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

func (session *Session) Execute(query string, args ...interface{}) (*Rows, error) {
	stmt := newSimpleStatement(query, len(args))
	defer stmt.Close()

	stmt.bind(args...)
	return session.Exec(stmt)
}

func (session *Session) Exec(stmt *Statement) (*Rows, error) {
	future := session.ExecAsync(stmt)
	defer future.Close()

	if err := future.Error(); err != nil {
		fmt.Printf("Execute: %s\r\n", err.Error())
		return nil, err
	}
	future.Wait()
	return future.Result(), nil
}

func (session *Session) ExecAsync(stmt *Statement) *Future {
	return async(func() *C.struct_CassFuture_ {
		return C.cass_session_execute(session.cptr, stmt.cptr)
	})
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

	return pstmt, nil
}

type PreparedStatement struct {
	cptr *C.struct_CassPrepared_
}

func (pstmt *PreparedStatement) Close() {
	C.cass_prepared_free(pstmt.cptr)
	pstmt.cptr = nil
}

func (pstmt *PreparedStatement) Bind(args ...interface{}) (*Statement, error) {
	stmt := newBoundStatement(pstmt)
	err := stmt.bind(args...)
	return stmt, err
}

type Future struct {
	cptr *C.struct_CassFuture_
}

func (future *Future) Error() error {
	if C.cass_future_error_code(future.cptr) == C.CASS_OK {
		return nil
	}
	var msg *C.char
	var sizet C.size_t

	C.cass_future_error_message(future.cptr, &msg, &sizet)
	return errors.New(C.GoStringN(msg, C.int(sizet)))
}

func (future *Future) Result() *Rows {
	result := new(Rows)
	result.cptr = C.cass_future_get_result(future.cptr)
	return result
}

// func (future *Future) Ready() bool {
// 	return C.cass_future_ready(future.cptr) == C.cass_true
// }

func (future *Future) Wait() {
	C.cass_future_wait(future.cptr)
}

func (future *Future) WaitTimed(timeout uint64) bool {
	return C.cass_future_wait_timed(future.cptr, C.cass_duration_t(timeout)) == C.cass_true
}

func (future *Future) Close() {
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

func (result *Rows) Close() {
	C.cass_result_free(result.cptr)
	result.cptr = nil
}

// func (result *Rows) RowCount() uint64 {
// 	return uint64(C.cass_result_row_count(result.cptr))
// }

func (result *Rows) ColumnCount() uint64 {
	return uint64(C.cass_result_column_count(result.cptr))
}

func (result *Rows) ColumnName(index int) string {
	var cStr *C.char
	var size C.size_t
	retc := C.cass_result_column_name(result.cptr, C.size_t(index), &cStr, &size)
	if retc == C.CASS_OK {
		return C.GoStringN(cStr, C.int(size))
	}
	return "UNKNOWN"
}

func (result *Rows) ColumnType(index int) string {
	return cassTypeName(C.cass_result_column_type(result.cptr, C.size_t(index)))
}

func (result *Rows) Next() bool {
	if result.iter == nil {
		result.iter = C.cass_iterator_from_result(result.cptr)
	}
	return C.cass_iterator_next(result.iter) != 0
}

func (result *Rows) Scan(args ...interface{}) error {
	if result.ColumnCount() < uint64(len(args)) {
		return errors.New("invalid argument count")
	}

	row := C.cass_iterator_get_row(result.iter)

	for i, v := range args {
		value := C.cass_row_get_column(row, C.size_t(i))
		casst := C.cass_result_column_type(result.cptr, C.size_t(i))

		if _, err := read(value, casst, v); err != nil {
			return newColumnError(result, i, v, err)
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
	return &Future{ptrFuture}
}

package cassandra

// #cgo LDFLAGS: -L/usr/local/lib -lcassandra
// #cgo CFLAGS: -I/usr/local/include
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import (
	"net"
	"unsafe"
)
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
	return cassType(C.cass_result_column_type(result.cptr, C.size_t(index)))
}

// func (result *Rows) HasMorePages() bool {
// 	return C.cass_result_has_more_pages(result.cptr) != 0
// }

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

	var retc C.CassError = C.CASS_OK

	for i, v := range args {
		value := C.cass_row_get_column(row, C.size_t(i))

		switch v := v.(type) {

		case *bool:
			var b C.cass_bool_t
			retc = C.cass_value_get_bool(value, &b)
			if retc == C.CASS_OK {
				*v = bool(b != 0)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *int8: // tinyint
			var i8 C.cass_int8_t
			retc = C.cass_value_get_int8(value, &i8)
			if retc == C.CASS_OK {
				*v = int8(i8)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *int16: // smallint
			var i16 C.cass_int16_t
			retc = C.cass_value_get_int16(value, &i16)
			if retc == C.CASS_OK {
				*v = int16(i16)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *int32:
			var i32 C.cass_int32_t
			retc = C.cass_value_get_int32(value, &i32)
			if retc == C.CASS_OK {
				*v = int32(i32)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *uint32:
			var u32 C.cass_uint32_t
			if retc = C.cass_value_get_uint32(value, &u32); retc != C.CASS_OK {
				return newColumnError(result, i, retc, v)
				return newError(retc, i)
			}
			*v = uint32(u32)

		case *int64:
			var i64 C.cass_int64_t
			retc = C.cass_value_get_int64(value, &i64)
			if retc == C.CASS_OK {
				*v = int64(i64)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *float32:
			var f32 C.cass_float_t
			retc = C.cass_value_get_float(value, &f32)
			if retc == C.CASS_OK {
				*v = float32(f32)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *float64:
			var f64 C.cass_double_t
			retc = C.cass_value_get_double(value, &f64)
			if retc == C.CASS_OK {
				*v = float64(f64)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *string:
			var str *C.char
			var size C.size_t
			retc := C.cass_value_get_string(value, &str, &size)
			if retc == C.CASS_OK {
				*v = C.GoStringN(str, C.int(size))
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *Time:
			var i64 C.cass_int64_t
			retc = C.cass_value_get_int64(value, &i64)
			if retc != C.CASS_OK {
				v.Nanos = int64(i64)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *Date:
			var u32 C.cass_uint32_t
			retc = C.cass_value_get_uint32(value, &u32)
			if retc == C.CASS_OK {
				v.Days = uint32(u32)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}
			v = nil

		case *Timestamp:
			var i64 C.cass_int64_t
			retc = C.cass_value_get_int64(value, &i64)
			if retc != C.CASS_OK {
				*v = Timestamp(i64)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *[]byte:
			var b *C.cass_byte_t
			var sizeT C.size_t
			retc := C.cass_value_get_bytes(value, &b, &sizeT)
			if retc == C.CASS_OK {
				*v = C.GoBytes(unsafe.Pointer(b), C.int(sizeT))
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *net.IP:
			var inet C.struct_CassInet_
			retc := C.cass_value_get_inet(value, &inet)
			if retc == C.CASS_OK {
				size := int(inet.address_length)
				ip := make([]byte, size)
				for i := 0; i < size; i++ {
					ip[i] = byte(inet.address[i])
				}
				*v = net.IP(ip)
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *UUID:
			var cuuid C.struct_CassUuid_
			retc := C.cass_value_get_uuid(value, &cuuid)
			if retc == C.CASS_OK {
				buf := (*C.char)(C.malloc(C.CASS_UUID_STRING_LENGTH))
				defer C.free(unsafe.Pointer(buf))

				C.cass_uuid_string(cuuid, buf)
				suuid := C.GoString(buf)

				uuid, retc := ParseUUID(suuid)
				if retc != nil {
					return retc
				}
				*v = uuid
			} else if retc != C.CASS_ERROR_LIB_NULL_VALUE {
				return newColumnError(result, i, retc, v)
			}

		case *int, *uint:
			return errors.New("usage of int/uint is discouraged as these numeric types have implementation specific sizes")
		default:
			return errors.New("unsupported type in Scan: " + reflect.TypeOf(v).String())
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

func cassType(kind C.CassValueType) string {
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

func newError(retc C.CassError, i int) error {
	msg := fmt.Sprintf("%s in argument %d", C.GoString(C.cass_error_desc(retc)), i)
	return errors.New(msg)
}

func newColumnError(rows *Rows, index int, retc C.CassError, v interface{}) error {
	columnName := rows.ColumnName(index)
	columnType := rows.ColumnType(index)
	argType := reflect.TypeOf(v).String()
	errMsg := fmt.Sprintf("%s (arg %d, type: %s, column: %s, type: %s)",
		C.GoString(C.cass_error_desc(retc)),
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

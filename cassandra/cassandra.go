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
	"strings"
)

type Cluster struct {
	cptr *C.struct_CassCluster_
}

func NewCluster(contactPoints ...string) *Cluster {
	cluster := new(Cluster)
	cluster.cptr = C.cass_cluster_new()
	cContactPoints := C.CString(strings.Join(contactPoints, ","))
	defer C.free(unsafe.Pointer(cContactPoints))
	C.cass_cluster_set_contact_points(cluster.cptr, cContactPoints)

	return cluster
}

func (cluster *Cluster) Close() {
	C.cass_cluster_free(cluster.cptr)
	cluster.cptr = nil
}

func (cluster *Cluster) Connect() (*Session, error) {
	session := new(Session)
	session.cptr = C.cass_session_new()

	future := async(func() *C.struct_CassFuture_ {
		return C.cass_session_connect(session.cptr, cluster.cptr)
	})
	defer future.Close()

	if err := future.Error(); err != nil {
		return nil, err
	}
	return session, nil
}

type Session struct {
	cptr *C.struct_CassSession_
}

func (session *Session) Close() {
	C.cass_session_free(session.cptr)
	session.cptr = nil
}

func (session *Session) Execute(query string, args ...interface{}) (*Result, error) {
	stmt := newSimpleStatement(query, len(args))
	defer stmt.Close()

	stmt.bind(args...)
	return session.Exec(stmt)
}

func (session *Session) Exec(stmt *Statement) (*Result, error) {
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

func (future *Future) Result() *Result {
	result := new(Result)
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

type Result struct {
	iter *C.struct_CassIterator_
	cptr *C.struct_CassResult_
}

func (result *Result) Close() {
	C.cass_result_free(result.cptr)
	result.cptr = nil
}

// func (result *Result) RowCount() uint64 {
// 	return uint64(C.cass_result_row_count(result.cptr))
// }

func (result *Result) ColumnCount() uint64 {
	return uint64(C.cass_result_column_count(result.cptr))
}

// func (result *Result) HasMorePages() bool {
// 	return C.cass_result_has_more_pages(result.cptr) != 0
// }

func (result *Result) Next() bool {
	if result.iter == nil {
		result.iter = C.cass_iterator_from_result(result.cptr)
	}
	return C.cass_iterator_next(result.iter) != 0
}

func (result *Result) Scan(args ...interface{}) error {

	if result.ColumnCount() > uint64(len(args)) {
		errors.New("invalid argument count")
	}

	row := C.cass_iterator_get_row(result.iter)

	var err C.CassError = C.CASS_OK

	for i, v := range args {
		value := C.cass_row_get_column(row, C.size_t(i))

		switch v := v.(type) {

		case *bool:
			var b C.cass_bool_t
			if err = C.cass_value_get_bool(value, &b); err != C.CASS_OK {
				return newError(err)
			}
			*v = bool(b != 0)

		case *int8: // tinyint
			var i8 C.cass_int8_t
			if err = C.cass_value_get_int8(value, &i8); err != C.CASS_OK {
				return newError(err)
			}
			*v = int8(i8)

		case *int16: // smallint
			var i16 C.cass_int16_t
			if err = C.cass_value_get_int16(value, &i16); err != C.CASS_OK {
				return newError(err)
			}
			*v = int16(i16)

		case *int32:
			var i32 C.cass_int32_t
			if err = C.cass_value_get_int32(value, &i32); err != C.CASS_OK {
				return newError(err)
			}
			*v = int32(i32)

		case *uint32:
			var u32 C.cass_uint32_t
			if err = C.cass_value_get_uint32(value, &u32); err != C.CASS_OK {
				return newError(err)
			}
			*v = uint32(u32)

		case *int64:
			var i64 C.cass_int64_t
			if err = C.cass_value_get_int64(value, &i64); err != C.CASS_OK {
				return newError(err)
			}
			*v = int64(i64)

		case *float32:
			var f32 C.cass_float_t
			if err = C.cass_value_get_float(value, &f32); err != C.CASS_OK {
				return newError(err)
			}
			*v = float32(f32)

		case *float64:
			var f64 C.cass_double_t
			if err = C.cass_value_get_double(value, &f64); err != C.CASS_OK {
				return newError(err)
			}
			*v = float64(f64)

		case *string:
			var str *C.char
			var sizeT C.size_t

			if err := C.cass_value_get_string(value, &str, &sizeT); err != C.CASS_OK {
				return newError(err)
			}
			*v = C.GoStringN(str, C.int(sizeT))

		case *Time:
			var i64 C.cass_int64_t
			err = C.cass_value_get_int64(value, &i64)
			if err != C.CASS_OK {
				if err != C.CASS_ERROR_LIB_NULL_VALUE {
					return newError(err)
				}
			} else {
				v.Nanos = int64(i64)
			}

		case *Date:
			var u32 C.cass_uint32_t
			if err = C.cass_value_get_uint32(value, &u32); err != C.CASS_OK {
				if err != C.CASS_ERROR_LIB_NULL_VALUE {
					return newError(err)
				}
			} else {
				v.Days = uint32(u32)
			}

		case *Timestamp:
			var i64 C.cass_int64_t
			err = C.cass_value_get_int64(value, &i64)
			if err != C.CASS_OK {
				if err != C.CASS_ERROR_LIB_NULL_VALUE {
					return newError(err)
				}
			} else {
				*v = Timestamp(i64)
			}

		case *[]byte:
			var b *C.cass_byte_t
			var sizeT C.size_t
			if err := C.cass_value_get_bytes(value, &b, &sizeT); err != C.CASS_OK {
				return newError(err)
			}
			*v = C.GoBytes(unsafe.Pointer(b), C.int(sizeT))

		case *net.IP:
			var inet C.struct_CassInet_

			if err := C.cass_value_get_inet(value, &inet); err != C.CASS_OK {
				return newError(err)
			}
			size := int(inet.address_length)
			ip := make([]byte, size)
			for i := 0; i < size; i++ {
				ip[i] = byte(inet.address[i])
			}
			*v = net.IP(ip)

		case *UUID:
			var cUuid C.struct_CassUuid_

			if err := C.cass_value_get_uuid(value, &cUuid); err != C.CASS_OK {
				return newError(err)
			}
			buf := (*C.char)(C.malloc(37))
			defer C.free(unsafe.Pointer(buf))
			C.cass_uuid_string(cUuid, buf)

			uuid := C.GoString(buf)
			u, err := ParseUUID(uuid)
			if err != nil {
				return err
			}
			*v = u
		default:
			return errors.New("unsupported type in Scan: " + reflect.TypeOf(v).String())
		}
	}

	return nil
}

func newError(err C.CassError) error {
	return errors.New(C.GoString(C.cass_error_desc(err)))
}

type statement struct {
	cptr *C.struct_CassStatement_
}

func async(f func() *C.struct_CassFuture_) *Future {
	ptrFuture := f()
	return &Future{ptrFuture}
}

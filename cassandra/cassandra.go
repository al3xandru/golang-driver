package cassandra

// #cgo LDFLAGS: -L/usr/local/lib -lcassandra
// #cgo CFLAGS: -I/usr/local/include
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)
import "unsafe"

//import "errors"
//import "reflect"

type Cluster struct {
	cptr *C.struct_CassCluster_
}

type Session struct {
	cptr *C.struct_CassSession_
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

	future := NewFuture(func() *C.struct_CassFuture_ {
		return C.cass_session_connect(session.cptr, cluster.cptr)
	})
	defer future.Close()

	if err := future.Error(); err != nil {
		return nil, err
	}
	return session, nil
}

func (session *Session) Close() {
	C.cass_session_free(session.cptr)
	session.cptr = nil
}

func (session *Session) Execute(query string, args ...interface{}) (*Result, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	stmt := C.cass_statement_new(cQuery, C.size_t(len(args)))
	defer C.cass_statement_free(stmt)

	future := NewFuture(func() *C.struct_CassFuture_ {
		return C.cass_session_execute(session.cptr, stmt)
	})
	defer future.Close()

	if err := future.Error(); err != nil {
		fmt.Printf("Execute: %s\r\n", err.Error())
		return nil, err
	}
	future.Wait()
	return future.Result(), nil
}

type Future struct {
	cptr *C.struct_CassFuture_
}

type FutureFunc func() *C.struct_CassFuture_

func NewFuture(f FutureFunc) *Future {
	ptrFuture := f()
	return &Future{ptrFuture}
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

func (future *Future) Ready() bool {
	return C.cass_future_ready(future.cptr) == C.cass_true
}

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

func (result *Result) RowCount() uint64 {
	return uint64(C.cass_result_row_count(result.cptr))
}

func (result *Result) ColumnCount() uint64 {
	return uint64(C.cass_result_column_count(result.cptr))
}

func (result *Result) HasMorePages() bool {
	return C.cass_result_has_more_pages(result.cptr) != 0
}

func (result *Result) Next() bool {
	if result.iter == nil {
		result.iter = C.cass_iterator_from_result(result.cptr)
	}
	return C.cass_iterator_next(result.iter) != 0
}

func (result *Result) Scan(args ...interface{}) error {

	if result.ColumnCount() != uint64(len(args)) {
		errors.New("invalid argument count")
	}

	row := C.cass_iterator_get_row(result.iter)

	var err C.CassError = C.CASS_OK

	for i, v := range args {
		value := C.cass_row_get_column(row, C.size_t(i))

		switch v := v.(type) {

		case *string:
			var str *C.char
			var sizeT C.size_t

			if err := C.cass_value_get_string(value, &str, &sizeT); err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = C.GoStringN(str, C.int(sizeT))
		//case *[]byte:
		////var b C.CassBytes
		////err = C.cass_value_get_bytes(value, &b)
		////if err != C.CASS_OK {
		////return errors.New(C.GoString(C.cass_error_desc(err)))
		////}
		////v = C.GoBytes(unsafe.Pointer(b.data), C.int(b.size))
		//var b C.cass_byte_t
		//var size C.size_t
		//err = C.cass_value_get_bytes(value, &b, &size)
		//if err != C.CASS_OK {
		//return errors.New(C.GoString(C.cass_error_desc(err)))
		//}
		//*v = C.GoBytes(unsafe.Pointer(b), C.int(size))
		case *int8: // tinyint
			var i8 C.cass_int8_t
			if err = C.cass_value_get_int8(value, &i8); err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = int8(i8)
		case *int16: // smallint
			var i16 C.cass_int16_t
			if err = C.cass_value_get_int16(value, &i16); err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = int16(i16)
		case *int32:
			var i32 C.cass_int32_t
			err = C.cass_value_get_int32(value, &i32)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = int32(i32)
		case *uint32:
			var u32 C.cass_uint32_t
			if err = C.cass_value_get_uint32(value, &u32); err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = uint32(u32)
		case *int64:
			var i64 C.cass_int64_t
			err = C.cass_value_get_int64(value, &i64)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = int64(i64)

		case *float32:
			var f32 C.cass_float_t
			err = C.cass_value_get_float(value, &f32)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = float32(f32)

		case *float64:
			var f64 C.cass_double_t
			err = C.cass_value_get_double(value, &f64)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = float64(f64)

		case *bool:
			var b C.cass_bool_t
			err = C.cass_value_get_bool(value, &b)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = bool(b != 0)
		case *Time:
			var i64 C.cass_int64_t
			err = C.cass_value_get_int64(value, &i64)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			v.Nanos = int64(i64)

		case *Date:
			var u32 C.cass_uint32_t
			if err = C.cass_value_get_uint32(value, &u32); err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}

			v.Days = uint32(u32)

		case *Timestamp:
			var i64 C.cass_int64_t
			err = C.cass_value_get_int64(value, &i64)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = Timestamp(i64)

		default:
			return errors.New("unsupported type in Scan: " + reflect.TypeOf(v).String())
		}
	}

	return nil
}

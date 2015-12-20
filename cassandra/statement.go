package cassandra

// #cgo LDFLAGS: -L/usr/local/lib -lcassandra
// #cgo CFLAGS: -I/usr/local/include
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import (
	"errors"
	"net"
	"unsafe"
)

type Statement struct {
	cptr *C.struct_CassStatement_
}

func (stmt *Statement) Close() {
	C.cass_statement_free(stmt.cptr)
	stmt.cptr = nil
}

// TODO: uuid, timeuuid, inet, []byte, decimal, collections, tuple, udt
func (stmt *Statement) bind(args ...interface{}) error {
	var cerr C.CassError = C.CASS_OK

	for i, v := range args {
		switch v := v.(type) {
		case bool:
			var val byte = 0
			if v {
				val = 1
			}
			cerr = C.cass_statement_bind_bool(stmt.cptr, C.size_t(i), C.cass_bool_t(val))
		case float32:
			cerr = C.cass_statement_bind_float(stmt.cptr, C.size_t(i), C.cass_float_t(v))
		case float64:
			cerr = C.cass_statement_bind_double(stmt.cptr, C.size_t(i), C.cass_double_t(v))
		case int8:
			cerr = C.cass_statement_bind_int8(stmt.cptr, C.size_t(i), C.cass_int8_t(v))
		case int16:
			cerr = C.cass_statement_bind_int16(stmt.cptr, C.size_t(i), C.cass_int16_t(v))
		case int32:
			cerr = C.cass_statement_bind_int32(stmt.cptr, C.size_t(i), C.cass_int32_t(v))
		case int64:
			cerr = C.cass_statement_bind_int64(stmt.cptr, C.size_t(i), C.cass_int64_t(v))
		case nil:
			cerr = C.cass_statement_bind_null(stmt.cptr, C.size_t(i))
		case uint32:
			cerr = C.cass_statement_bind_uint32(stmt.cptr, C.size_t(i), C.cass_uint32_t(v))
		case Date:
			cerr = C.cass_statement_bind_uint32(stmt.cptr, C.size_t(i), C.cass_uint32_t(v.Days))
		case Time:
			cerr = C.cass_statement_bind_int64(stmt.cptr, C.size_t(i), C.cass_int64_t(v))
		case Timestamp:
			cerr = C.cass_statement_bind_int64(stmt.cptr, C.size_t(i), C.cass_int64_t(v))
		case string:
			cStr := C.CString(v)
			defer C.free(unsafe.Pointer(cStr))
			cerr = C.cass_statement_bind_string(stmt.cptr, C.size_t(i), cStr)
		case []byte:
			cerr = C.cass_statement_bind_bytes(stmt.cptr, C.size_t(i),
				(*C.cass_byte_t)(unsafe.Pointer(&v)), C.size_t(len(v)))
		case net.IP:
			b := []byte(v)
			var cInet C.struct_CassInet_
			cInet.address_length = C.cass_uint8_t(len(b))
			for j, _ := range b {
				cInet.address[j] = C.cass_uint8_t(b[j])
			}
			cerr = C.cass_statement_bind_inet(stmt.cptr, C.size_t(i), cInet)
		}
	}
	if cerr != C.CASS_OK {
		return errors.New(C.GoString(C.cass_error_desc(cerr)))
	}
	return nil
}

func newSimpleStatement(query string, paramLen int) *Statement {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	stmt := new(Statement)
	stmt.cptr = C.cass_statement_new(cQuery, C.size_t(paramLen))

	return stmt
}

func newBoundStatement(pstmt *PreparedStatement) *Statement {
	stmt := new(Statement)
	stmt.cptr = C.cass_prepared_bind(pstmt.cptr)

	return stmt
}

package cassandra

// #cgo LDFLAGS: -L/usr/local/lib -lcassandra
// #cgo CFLAGS: -I/usr/local/include
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import "unsafe"

type Statement struct {
	cptr              *C.struct_CassStatement_
	session           *Session
	pstmt             *PreparedStatement
	consistency       Consistency
	serialConsistency Consistency
	Args              []interface{}
}

func (stmt *Statement) WithConsistency(c Consistency) *Statement {
	stmt.consistency = c
	return stmt
}

func (stmt *Statement) WithSerialConsistency(c Consistency) *Statement {
	stmt.serialConsistency = c
	return stmt
}

// func (stmt *Statement) WithTimestamp(ts int) *Statement          {}
// func (stmt *Statement) WithCustomPayload(payload int) *Statement {}
// func (stmt *Statement) WithPagingToken(token int) *Statement     {}

func (stmt *Statement) Close() {
	C.cass_statement_free(stmt.cptr)
	stmt.cptr = nil
}

func (stmt *Statement) Exec() (*Rows, error) {
	future := stmt.ExecAsync()
	defer future.Close()

	if err := future.Error(); err != nil {
		return nil, err
	}
	return future.Result(), nil
}

func (stmt *Statement) ExecAsync() *Future {
	if stmt.consistency != unset {
		retc := C.cass_statement_set_consistency(stmt.cptr, stmt.consistency.toC())
		if retc != C.CASS_OK {
			// return an error Future
			return &Future{err: newError(retc)}
		}
	}
	if stmt.serialConsistency != unset {
		retc := C.cass_statement_set_serial_consistency(stmt.cptr, stmt.serialConsistency.toC())
		if retc != C.CASS_OK {
			// return an error Future
			return &Future{err: newError(retc)}
		}
	}

	return async(func() *C.struct_CassFuture_ {
		return C.cass_session_execute(stmt.session.cptr, stmt.cptr)
	})
}

func (stmt *Statement) bind(args ...interface{}) error {
	stmt.Args = args
	for i, v := range args {
		if err := write(stmt, v, i, stmt.dataType(i)); err != nil {
			return err
		}
	}
	return nil
}

func newSimpleStatement(session *Session, query string, paramLen int) *Statement {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	stmt := new(Statement)
	stmt.cptr = C.cass_statement_new(cQuery, C.size_t(paramLen))
	stmt.session = session
	stmt.consistency = unset
	stmt.serialConsistency = unset

	return stmt
}

func newBoundStatement(pstmt *PreparedStatement) *Statement {
	stmt := new(Statement)
	stmt.cptr = C.cass_prepared_bind(pstmt.cptr)
	stmt.pstmt = pstmt
	stmt.session = pstmt.session
	stmt.consistency = unset
	stmt.serialConsistency = unset

	return stmt
}

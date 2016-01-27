package cassandra

// #cgo LDFLAGS: -L/usr/local/lib -lcassandra
// #cgo CFLAGS: -I/usr/local/include
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import "unsafe"

import "errors"

type Session struct {
	cptr    *C.struct_CassSession_
	Cluster *Cluster
}

func (session *Session) Close() {
	C.cass_session_free(session.cptr)
	session.cptr = nil
	session.Cluster = nil
}

// Executes the given query and returns either the resulting
// *Rows or an error.
func (session *Session) Exec(query string, args ...interface{}) (*Rows, error) {
	future := session.ExecAsync(query, args...)
	defer future.Close()

	if err := future.Error(); err != nil {
		return nil, err
	}

	return future.Result(), nil
}

// Returns a *Statement which can be used to customize the CL,
// Serial CL, etc. The *Statement **must** be Close() once done.
func (session *Session) Query(query string, args ...interface{}) (*Statement, error) {
	stmt := newSimpleStatement(session, query, len(args))

	if err := stmt.bind(args...); err != nil {
		return nil, err
	}

	return stmt, nil
}

// Executes the given query asynchronously and returns a *Future
// that can be used to retrieve the results (or error).
func (session *Session) ExecAsync(query string, args ...interface{}) *Future {
	stmt := newSimpleStatement(session, query, len(args))
	defer stmt.Close()

	if err := stmt.bind(args...); err != nil {
		return &Future{err: err}
	}

	return stmt.ExecAsync()
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
	case LOCAL_ONE:
		return C.CassConsistency(C.CASS_CONSISTENCY_LOCAL_ONE)
	case LOCAL_QUORUM:
		return C.CassConsistency(C.CASS_CONSISTENCY_LOCAL_QUORUM)
	case EACH_QUORUM:
		return C.CassConsistency(C.CASS_CONSISTENCY_EACH_QUORUM)
	case SERIAL:
		return C.CassConsistency(C.CASS_CONSISTENCY_SERIAL)
	case LOCAL_SERIAL:
		return C.CassConsistency(C.CASS_CONSISTENCY_LOCAL_SERIAL)
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
	defer stmt.Close()

	stmt.WithConsistency(pstmt.consistency)
	stmt.WithSerialConsistency(pstmt.serialConsistency)

	if err := stmt.bind(args...); err != nil {
		return &Future{err: err}
	}

	return stmt.ExecAsync()
}

func (pstmt *PreparedStatement) Query(args ...interface{}) (*Statement, error) {
	stmt := newBoundStatement(pstmt)
	stmt.WithConsistency(pstmt.consistency)
	stmt.WithSerialConsistency(pstmt.consistency)

	if err := stmt.bind(args...); err != nil {
		return nil, err
	}

	return stmt, nil
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

func (rows *Rows) ColumnType(index int) CassType {
	return CassTypeFromDataType(C.cass_result_column_data_type(rows.cptr, C.size_t(index)))
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
		pos := C.size_t(i)
		value := C.cass_row_get_column(row, pos)
		ctype := CassTypeFromDataType(
			C.cass_result_column_data_type(rows.cptr, pos))

		if _, err := read(value, ctype, v); err != nil {
			return newColumnError(rows, i, v, err)
		}
	}

	return nil
}

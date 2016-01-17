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
)

type cassDataType *C.struct_CassDataType_

func valueType(cdt cassDataType) C.CassValueType {
	if cdt == nil {
		return CASS_VALUE_TYPE_UNKNOWN
	}

	return C.cass_data_type_type(cdt)
}

func (stmt *Statement) dataType(index int) cassDataType {
	if stmt.pstmt == nil {
		return nil
	}
	return cassDataType(C.cass_prepared_parameter_data_type(stmt.pstmt.cptr,
		C.size_t(index)))
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

// CPP error code to error
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

func async(f func() *C.struct_CassFuture_) *Future {
	ptrFuture := f()
	return &Future{cptr: ptrFuture}
}

package cassandra

// #cgo LDFLAGS: -L/usr/local/lib -lcassandra
// #cgo CFLAGS: -I/usr/local/include
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"
)

type CassType struct {
	PrimaryType  int
	SubTypes     []CassType
	SubTypeNames map[string]int
}

func newCassType(kind int, subTypes ...int) CassType {
	ctype := new(CassType)
	ctype.PrimaryType = kind
	if len(subTypes) > 0 {
		subs := make([]CassType, len(subTypes))
		for i, s := range subTypes {
			subs[i] = newCassType(s)
		}
		ctype.SubTypes = subs
	}
	return *ctype
}

func CassTypeFromDataType(cdt cassDataType) CassType {
	if cdt == nil {
		return CASS_UNKNOWN
	}
	cvt := valueType(cdt)
	switch cvt {
	case CASS_VALUE_TYPE_LIST, CASS_VALUE_TYPE_SET:
		ctype := new(CassType)
		ctype.PrimaryType = int(cvt)
		ctype.SubTypes = make([]CassType, 1)
		ctype.SubTypes[0] = CassTypeFromDataType(C.cass_data_type_sub_data_type(cdt, 0))

		return *ctype

	case CASS_VALUE_TYPE_MAP:
		ctype := new(CassType)
		ctype.PrimaryType = CASS_VALUE_TYPE_MAP
		ctype.SubTypes = make([]CassType, 2)
		for i, _ := range ctype.SubTypes {
			ctype.SubTypes[i] = CassTypeFromDataType(
				C.cass_data_type_sub_data_type(cdt, C.size_t(i)))
		}

		return *ctype

	case CASS_VALUE_TYPE_TUPLE:
		ctype := new(CassType)
		ctype.PrimaryType = CASS_VALUE_TYPE_TUPLE
		ctype.SubTypes = make([]CassType, int(C.cass_data_sub_type_count(cdt)))
		for i, _ := range ctype.SubTypes {
			ctype.SubTypes[i] = CassTypeFromDataType(
				C.cass_data_type_sub_data_type(cdt, C.size_t(i)))
		}

		return *ctype

	// incomplete for now
	case CASS_VALUE_TYPE_UDT:
		return CASS_UDT

	case CASS_VALUE_TYPE_ASCII:
		return CASS_ASCII
	case CASS_VALUE_TYPE_TEXT:
		return CASS_TEXT
	case CASS_VALUE_TYPE_VARCHAR:
		return CASS_VARCHAR
	case CASS_VALUE_TYPE_BIGINT:
		return CASS_BIGINT
	case CASS_VALUE_TYPE_INT:
		return CASS_INT
	case CASS_VALUE_TYPE_SMALL_INT:
		return CASS_SMALLINT
	case CASS_VALUE_TYPE_TINY_INT:
		return CASS_TINYINIT
	case CASS_VALUE_TYPE_VARINT:
		return CASS_VARINT
	case CASS_VALUE_TYPE_BLOB:
		return CASS_BLOB
	case CASS_VALUE_TYPE_BOOLEAN:
		return CASS_BOOLEAN
	case CASS_VALUE_TYPE_DECIMAL:
		return CASS_DECIMAL
	case CASS_VALUE_TYPE_DOUBLE:
		return CASS_DOUBLE
	case CASS_VALUE_TYPE_FLOAT:
		return CASS_FLOAT
	case CASS_VALUE_TYPE_TIMESTAMP:
		return CASS_TIMESTAMP
	case CASS_VALUE_TYPE_DATE:
		return CASS_DATE
	case CASS_VALUE_TYPE_TIME:
		return CASS_TIME
	case CASS_VALUE_TYPE_UUID:
		return CASS_UUID
	case CASS_VALUE_TYPE_TIMEUUID:
		return CASS_TIMEUUID
	case CASS_VALUE_TYPE_INET:
		return CASS_INET
	// counter, custom
	default:
		return newCassType(int(cvt))
	}
}

// Specialize a collection type (list, set, map, tuple) with the
// type(s) of its elements
func (ct CassType) Subtype(subTypes ...CassType) CassType {
	return CassType{PrimaryType: ct.PrimaryType, SubTypes: subTypes}
}

func (ct *CassType) Name() string {
	switch ct.PrimaryType {
	case CASS_VALUE_TYPE_LIST:
		if len(ct.SubTypes) > 0 {
			return fmt.Sprintf("list<%s>", ct.SubTypes[0].Name())
		}
		return "list"
	case CASS_VALUE_TYPE_SET:
		if len(ct.SubTypes) > 0 {
			return fmt.Sprintf("set<%s>", ct.SubTypes[0].Name())
		}
		return "set"
	case CASS_VALUE_TYPE_MAP:
		if len(ct.SubTypes) > 1 {
			return fmt.Sprintf("map<%s, %s>", ct.SubTypes[0].Name(),
				ct.SubTypes[1].Name())
		}
		return "map"
	case CASS_VALUE_TYPE_TUPLE:
		names := make([]string, len(ct.SubTypes))
		for i, st := range ct.SubTypes {
			names[i] = st.Name()
		}
		return fmt.Sprintf("tuple<%s>", strings.Join(names, ", "))
	case CASS_VALUE_TYPE_ASCII:
		return "ascii"
	case CASS_VALUE_TYPE_TEXT:
		return "text"
	case CASS_VALUE_TYPE_VARCHAR:
		return "varchar"
	case CASS_VALUE_TYPE_BIGINT:
		return "bigint"
	case CASS_VALUE_TYPE_INT:
		return "int"
	case CASS_VALUE_TYPE_SMALL_INT:
		return "smallint"
	case CASS_VALUE_TYPE_TINY_INT:
		return "tinyint"
	case CASS_VALUE_TYPE_VARINT:
		return "varint"
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
	case CASS_VALUE_TYPE_TIMESTAMP:
		return "timestamp"
	case CASS_VALUE_TYPE_DATE:
		return "date"
	case CASS_VALUE_TYPE_TIME:
		return "time"
	case CASS_VALUE_TYPE_UUID:
		return "uuid"
	case CASS_VALUE_TYPE_TIMEUUID:
		return "timeuuid"
	case CASS_VALUE_TYPE_INET:
		return "inet"
	case CASS_VALUE_TYPE_UDT:
		return "udt"
	case CASS_VALUE_TYPE_CUSTOM:
		return "custom"
	default:
		return "UNKNOWN"
	}
}

func (ct *CassType) Eq(other CassType) bool {
	if ct.PrimaryType != other.PrimaryType {
		return false
	}
	if len(ct.SubTypes) != len(other.SubTypes) {
		return false
	}
	for idx, _ := range ct.SubTypes {
		if !ct.SubTypes[idx].Eq(other.SubTypes[idx]) {
			return false
		}
	}
	return true
}

func (ct *CassType) String() string {
	return ct.Name()
}

var (
	CASS_UNKNOWN   = newCassType(CASS_VALUE_TYPE_UNKNOWN)
	CASS_ASCII     = newCassType(CASS_VALUE_TYPE_ASCII)
	CASS_BIGINT    = newCassType(CASS_VALUE_TYPE_BIGINT)
	CASS_BLOB      = newCassType(CASS_VALUE_TYPE_BLOB)
	CASS_BOOLEAN   = newCassType(CASS_VALUE_TYPE_BOOLEAN)
	CASS_DECIMAL   = newCassType(CASS_VALUE_TYPE_DECIMAL)
	CASS_DOUBLE    = newCassType(CASS_VALUE_TYPE_DOUBLE)
	CASS_FLOAT     = newCassType(CASS_VALUE_TYPE_FLOAT)
	CASS_INT       = newCassType(CASS_VALUE_TYPE_INT)
	CASS_TEXT      = newCassType(CASS_VALUE_TYPE_TEXT)
	CASS_TIMESTAMP = newCassType(CASS_VALUE_TYPE_TIMESTAMP)
	CASS_UUID      = newCassType(CASS_VALUE_TYPE_UUID)
	CASS_VARCHAR   = newCassType(CASS_VALUE_TYPE_VARCHAR)
	CASS_VARINT    = newCassType(CASS_VALUE_TYPE_VARINT)
	CASS_TIMEUUID  = newCassType(CASS_VALUE_TYPE_TIMEUUID)
	CASS_INET      = newCassType(CASS_VALUE_TYPE_INET)
	CASS_DATE      = newCassType(CASS_VALUE_TYPE_DATE)
	CASS_TIME      = newCassType(CASS_VALUE_TYPE_TIME)
	CASS_SMALLINT  = newCassType(CASS_VALUE_TYPE_SMALL_INT)
	CASS_TINYINIT  = newCassType(CASS_VALUE_TYPE_TINY_INT)
	// collections
	CASS_LIST  = newCassType(CASS_VALUE_TYPE_LIST)
	CASS_SET   = newCassType(CASS_VALUE_TYPE_SET)
	CASS_MAP   = newCassType(CASS_VALUE_TYPE_MAP)
	CASS_TUPLE = newCassType(CASS_VALUE_TYPE_TUPLE)
	CASS_UDT   = newCassType(CASS_VALUE_TYPE_UDT)
)

type cassDataType *C.struct_CassDataType_

func valueType(cdt cassDataType) C.CassValueType {
	if cdt == nil {
		return CASS_VALUE_TYPE_UNKNOWN
	}

	return C.cass_data_type_type(cdt)
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
		columnType.Name())
	return errors.New(errMsg)
}

func async(f func() *C.struct_CassFuture_) *Future {
	ptrFuture := f()
	return &Future{cptr: ptrFuture}
}

func import2Complement(b []byte, dst *big.Int) *big.Int {
	dst = dst.SetBytes(b)
	if (b[0] & 0x80) == 0x80 {
		sub := big.NewInt(8)
		sub = sub.Mul(sub, big.NewInt(int64(len(b))))
		sub = sub.Exp(big.NewInt(2), sub, big.NewInt(0))
		dst = dst.Sub(dst, sub)
	}

	return dst
}

func export2Complement(src *big.Int) []byte {
	switch src.Sign() {
	case 0:
		return []byte{0}
	case -1:
		bytesCount := src.BitLen()/8 + 1
		for i := src.BitLen() - 1; i > -1; i-- {
			if src.Bit(i) == 1 {
				if i == (8*(bytesCount-1))-1 {
					bytesCount--
				}
				break
			}
		}
		add := big.NewInt(8)
		add = add.Mul(add, big.NewInt(int64(bytesCount)))
		add = add.Exp(big.NewInt(2), add, big.NewInt(0))
		src = src.Add(src, add)
		return src.Bytes()
	default:
		bytesCount := (src.BitLen() + 7) / 8
		if src.Bit((8*bytesCount)-1) == 1 {
			buf := make([]byte, bytesCount+1)
			buf[0] = 0
			copy(buf[1:], src.Bytes())
			return buf
		} else {
			return src.Bytes()
		}
	}
}

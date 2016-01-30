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
)

// Private API for CassType
func newCassType(kind int, subTypes ...int) CassType {
	ctype := new(CassType)
	ctype.primary = kind
	if len(subTypes) > 0 {
		subs := make([]CassType, len(subTypes))
		for i, s := range subTypes {
			subs[i] = newCassType(s)
		}
		ctype.subtypes = subs
	}
	return *ctype
}

func cassTypeFromCassDataType(cdt cassDataType) CassType {
	if cdt == nil {
		return CUnknown
	}
	cvt := valueType(cdt)
	switch cvt {
	case CASS_VALUE_TYPE_LIST, CASS_VALUE_TYPE_SET:
		ctype := new(CassType)
		ctype.primary = int(cvt)
		ctype.subtypes = make([]CassType, 1)
		ctype.subtypes[0] = cassTypeFromCassDataType(C.cass_data_type_sub_data_type(cdt, 0))

		return *ctype

	case CASS_VALUE_TYPE_MAP:
		ctype := new(CassType)
		ctype.primary = CASS_VALUE_TYPE_MAP
		ctype.subtypes = make([]CassType, 2)
		for i, _ := range ctype.subtypes {
			ctype.subtypes[i] = cassTypeFromCassDataType(
				C.cass_data_type_sub_data_type(cdt, C.size_t(i)))
		}

		return *ctype

	case CASS_VALUE_TYPE_TUPLE:
		ctype := new(CassType)
		ctype.primary = CASS_VALUE_TYPE_TUPLE
		ctype.subtypes = make([]CassType, int(C.cass_data_sub_type_count(cdt)))
		for i, _ := range ctype.subtypes {
			ctype.subtypes[i] = cassTypeFromCassDataType(
				C.cass_data_type_sub_data_type(cdt, C.size_t(i)))
		}

		return *ctype

	// incomplete for now
	case CASS_VALUE_TYPE_UDT:
		return CUdt

	case CASS_VALUE_TYPE_ASCII:
		return CAscii
	case CASS_VALUE_TYPE_TEXT:
		return CText
	case CASS_VALUE_TYPE_VARCHAR:
		return CVarchar
	case CASS_VALUE_TYPE_BIGINT:
		return CBigInt
	case CASS_VALUE_TYPE_INT:
		return CInt
	case CASS_VALUE_TYPE_SMALL_INT:
		return CSmallInt
	case CASS_VALUE_TYPE_TINY_INT:
		return CTinyInt
	case CASS_VALUE_TYPE_VARINT:
		return CVarint
	case CASS_VALUE_TYPE_BLOB:
		return CBlob
	case CASS_VALUE_TYPE_BOOLEAN:
		return CBoolean
	case CASS_VALUE_TYPE_DECIMAL:
		return CDecimal
	case CASS_VALUE_TYPE_DOUBLE:
		return CDouble
	case CASS_VALUE_TYPE_FLOAT:
		return CFloat
	case CASS_VALUE_TYPE_TIMESTAMP:
		return CTimestamp
	case CASS_VALUE_TYPE_DATE:
		return CDate
	case CASS_VALUE_TYPE_TIME:
		return CTime
	case CASS_VALUE_TYPE_UUID:
		return CUuid
	case CASS_VALUE_TYPE_TIMEUUID:
		return CTimeuuid
	case CASS_VALUE_TYPE_INET:
		return CInet
	// counter, custom
	default:
		return newCassType(int(cvt))
	}
}

func (ct CassType) equals(other CassType) bool {
	if ct.primary != other.primary {
		return false
	}
	if len(ct.subtypes) != len(other.subtypes) {
		return false
	}
	for idx, _ := range ct.subtypes {
		if !ct.subtypes[idx].equals(other.subtypes[idx]) {
			return false
		}
	}
	return true
}

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
		columnType.String())
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

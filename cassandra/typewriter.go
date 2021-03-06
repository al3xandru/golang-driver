package cassandra

// #cgo LDFLAGS: -L/usr/local/lib -lcassandra
// #cgo CFLAGS: -I/usr/local/include
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import (
	"fmt"
	"math"
	"math/big"
	"net"
	"reflect"
	"unsafe"
)

// this is the only function called from outside this source file
// it's not an exported function as it's used only internally
func write(stmt *Statement, value interface{}, index int, dataType CassType) error {
	if value == nil {
		if retc := C.cass_statement_bind_null(stmt.cptr, C.size_t(index)); retc != C.CASS_OK {
			return newError(retc)
		}
	}
	// fmt.Printf("write(%v %T)\n", value, value)
	tv, err := newCassTypedVal(value, dataType)
	if err != nil {
		return err
	}
	defer tv.Free()
	// DEBUG
	// ct := tv.Kind()
	// fmt.Printf("->%s.BindTo(%d)\n", ct.String(), index)
	return tv.BindTo(stmt, index)
}

// const maxUint = ^uint(0)
// const maxInt = int(maxUint >> 1)

type primitiveTypedVal struct {
	val  interface{}
	kind CassType
}

type collectionTypedVal struct {
	cptr     *C.struct_CassCollection_
	kind     CassType
	cassType C.CassValueType
}

type tupleTypedVal struct {
	cptr *C.struct_CassTuple_
	kind CassType
}

type typedValue interface {
	BindTo(dst interface{}, index int) error
	Kind() CassType
	Free()
}

func newCassTypedVal(value interface{}, dataType CassType) (typedValue, error) {
	// fmt.Printf("write(dataType=%s)\n", dataType.String())

	switch dataType.primary {
	case CASS_VALUE_TYPE_ASCII, CASS_VALUE_TYPE_TEXT, CASS_VALUE_TYPE_VARCHAR:
		return toText(value, dataType)
	case CASS_VALUE_TYPE_BOOLEAN:
		return toBool(value, dataType)
	case CASS_VALUE_TYPE_BIGINT:
		return toBigint(value, dataType)
	case CASS_VALUE_TYPE_INT:
		return toInt(value, dataType)
	case CASS_VALUE_TYPE_SMALL_INT:
		return toSmallInt(value, dataType)
	case CASS_VALUE_TYPE_TINY_INT:
		return toTinyInt(value, dataType)
	// case CASS_VALUE_TYPE_COUNTER, ,
	// 	return readInt(value, cassType, dst)
	case CASS_VALUE_TYPE_VARINT:
		return toVarint(value, dataType)
	case CASS_VALUE_TYPE_FLOAT:
		return toFloat(value, dataType)
	case CASS_VALUE_TYPE_DOUBLE:
		return toDouble(value, dataType)
	case CASS_VALUE_TYPE_DECIMAL:
		return toDecimal(value, dataType)
	case CASS_VALUE_TYPE_TIMESTAMP:
		return toTimestamp(value, dataType)
	case CASS_VALUE_TYPE_DATE:
		return toDate(value, dataType)
	case CASS_VALUE_TYPE_TIME:
		return toTime(value, dataType)
	case CASS_VALUE_TYPE_UUID, CASS_VALUE_TYPE_TIMEUUID:
		return toUUID(value, dataType)
	case CASS_VALUE_TYPE_INET:
		return toInet(value, dataType)
	case CASS_VALUE_TYPE_BLOB:
		return toBlob(value, dataType)
	case CASS_VALUE_TYPE_LIST:
		return toList(value, dataType)
	case CASS_VALUE_TYPE_SET:
		return toSet(value, dataType)
	case CASS_VALUE_TYPE_MAP:
		return toMap(value, dataType)
	case CASS_VALUE_TYPE_TUPLE:
		return toTuple(value, dataType)
		// case CASS_VALUE_TYPE_UDT:
		// 	return readUDT(value, cassType, dst)
	}

	switch value := value.(type) {
	case bool:
		return toBool(value, CBoolean)
	case int64:
		return toBigint(value, CBigInt)
	case int32:
		return toInt(value, CInt)
	case int16:
		return toSmallInt(value, CSmallInt)
	case int8:
		return toTinyInt(value, CTinyInt)
	case int:
		// must determine if it's 64 or 32
		if value < math.MinInt32 || value > math.MaxInt32 {
			return toBigint(value, CBigInt)
		} else {
			return toInt(value, CInt)
		}
	case *big.Int:
		return toVarint(value, CVarint)
	case float32:
		return toFloat(value, CFloat)
	case float64:
		return toDouble(value, CDouble)
	case *Decimal:
		return toDecimal(value, CDecimal)
	case string:
		return toText(value, CText)
	case UUID:
		switch value.Version() {
		case 1:
			return toUUID(value, CTimeuuid)
		case 4:
			return toUUID(value, CUuid)
		}
	case Date:
		return toDate(value, CDate)
	case Time:
		return toTime(value, CTime)
	case Timestamp:
		return toTimestamp(value, CTimestamp)
	case net.IP:
		return toInet(value, CInet)
	case []byte:
		return toBlob(value, CBlob)
	case setmarker:
		return toSet(value.value, CSet)
	case Tuple, *Tuple:
		return toTuple(value, CTuple)
	}
	// last attempt
	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Bool:
		return toBool(value, CBoolean)
	case reflect.Int64:
		return toBigint(value, CBigInt)
	case reflect.Int32:
		return toInt(value, CInt)
	case reflect.Int16:
		return toSmallInt(value, CSmallInt)
	case reflect.Int8:
		return toTinyInt(value, CTinyInt)
	case reflect.Int:
		// if maxInt > 2147483647 {
		if rVal.Int() < math.MinInt32 || rVal.Int() > math.MaxInt32 {
			return toBigint(rVal.Int(), CBigInt)
		} else {
			return toInt(rVal.Int(), CInt)
		}
	case reflect.Float32:
		return toFloat(value, CFloat)
	case reflect.Float64:
		return toDouble(value, CDouble)
	case reflect.String:
		return toText(value, CText)
	case reflect.Map:
		return toMap(value, CMap)
	case reflect.Slice, reflect.Array:
		return toList(value, CList)
	}

	return nil, fmt.Errorf("unknown type %T", value)
}

func toBool(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case bool:
		if value {
			return &primitiveTypedVal{1, cassType}, nil
		} else {
			return &primitiveTypedVal{0, cassType}, nil
		}
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Bool:
		if rVal.Bool() {
			return &primitiveTypedVal{1, cassType}, nil
		} else {
			return &primitiveTypedVal{0, cassType}, nil
		}
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toBigint(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case int64:
		return &primitiveTypedVal{value, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int64:
		return &primitiveTypedVal{rVal.Int(), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toInt(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case int32, int:
		return &primitiveTypedVal{value, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int32, reflect.Int:
		return &primitiveTypedVal{rVal.Int(), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toSmallInt(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case int16:
		return &primitiveTypedVal{value, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int16:
		return &primitiveTypedVal{rVal.Int(), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toTinyInt(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case int8:
		return &primitiveTypedVal{value, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int8:
		return &primitiveTypedVal{rVal.Int(), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toVarint(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case *big.Int:
		buf := export2Complement(value)
		return &primitiveTypedVal{buf, cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toFloat(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case float32:
		return &primitiveTypedVal{value, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Float32:
		return &primitiveTypedVal{rVal.Float(), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toDouble(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case float64:
		return &primitiveTypedVal{value, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Float64:
		return &primitiveTypedVal{rVal.Float(), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toDecimal(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case *Decimal:
		return &primitiveTypedVal{value, cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

// Could allow string too
func toUUID(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	if value, ok := value.(UUID); ok {
		return &primitiveTypedVal{value, cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toText(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case string:
		return &primitiveTypedVal{value, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.String:
		return &primitiveTypedVal{rVal.String(), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toDate(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case Date:
		return &primitiveTypedVal{value.days, cassType}, nil
	case uint32:
		return &primitiveTypedVal{value, cassType}, nil
	case string:
		date, err := ParseDate(value)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %T (%s) into %s",
				value, value, cassType.String())
		}
		return &primitiveTypedVal{date.days, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Uint32:
		return &primitiveTypedVal{rVal.Uint(), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toTime(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case Time:
		return &primitiveTypedVal{int64(value), cassType}, nil
	case int64:
		return &primitiveTypedVal{value, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int64:
		return &primitiveTypedVal{rVal.Int(), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toTimestamp(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case Timestamp:
		return &primitiveTypedVal{value.secondsSinceEpoch, cassType}, nil
	case int64:
		return &primitiveTypedVal{value, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int64:
		return &primitiveTypedVal{rVal.Int(), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toInet(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case net.IP:
		return &primitiveTypedVal{[]byte(value), cassType}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toBlob(value interface{}, cassType CassType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case []byte:
		return &primitiveTypedVal{value, cassType}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Slice, reflect.Array:
		if rVal.Type().Elem().Kind() == reflect.Uint8 {
			return &primitiveTypedVal{rVal.Bytes(), cassType}, nil
		}
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassType.String())
}

func toTuple(value interface{}, dataType CassType) (*tupleTypedVal, error) {
	switch value := value.(type) {
	case *Tuple:
		sz := len(value.Kind().subtypes)
		// FIXME: cptr must be freed C.cass_tuple_free(cptr)
		cptr := C.cass_tuple_new(C.size_t(sz))
		ttv := &tupleTypedVal{cptr, dataType}

		tupleVals := value.Values()

		for idx := 0; idx < sz; idx++ {
			if idx >= len(tupleVals) {
				C.cass_tuple_set_null(cptr, C.size_t(idx))
				continue
			}
			v := value.Get(idx)
			if v == nil {
				C.cass_tuple_set_null(cptr, C.size_t(idx))
				continue
			}
			tv, err := newCassTypedVal(v, value.Kind().subtypes[idx])
			if err != nil {
				return nil, err
			}
			defer tv.Free()
			if err = tv.BindTo(ttv, idx); err != nil {
				return nil, err
			}
		}
		return ttv, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, dataType.String())
}

func toList(value interface{}, dataType CassType) (*collectionTypedVal, error) {
	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Slice, reflect.Array:
		col := C.cass_collection_new(C.CASS_COLLECTION_TYPE_LIST, C.size_t(rVal.Len()))
		ctv := &collectionTypedVal{col, dataType, CASS_VALUE_TYPE_LIST}

		elemDataType := CUnknown
		if len(dataType.subtypes) > 0 {
			elemDataType = dataType.subtypes[0]
		}

		idx := 0
		for idx < rVal.Len() {
			tv, err := newCassTypedVal(rVal.Index(idx).Interface(), elemDataType)
			if err != nil {
				return nil, err
			}
			defer tv.Free()
			if elemDataType.Equals(CUnknown) {
				elemDataType = tv.Kind()
				ctv.kind = ctv.kind.Specialize(elemDataType)
			}
			if err = tv.BindTo(ctv, -1); err != nil {
				return nil, err
			}
			idx += 1
		}
		return ctv, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, dataType.String())
}

func toMap(value interface{}, dataType CassType) (*collectionTypedVal, error) {
	rVal := reflect.ValueOf(value)
	if rVal.Type().Kind() != reflect.Map {
		return nil, fmt.Errorf("cannot convert %T into %s", value, dataType.String())
	}
	col := C.cass_collection_new(C.CASS_COLLECTION_TYPE_MAP, C.size_t(rVal.Len()))
	ctv := &collectionTypedVal{col, dataType, CASS_VALUE_TYPE_MAP}

	var keyDataType, valDataType CassType = CUnknown, CUnknown
	if len(dataType.subtypes) > 0 {
		// keyDataType = cassDataType(C.cass_data_type_sub_data_type(dataType, 0))
		// valDataType = cassDataType(C.cass_data_type_sub_data_type(dataType, 1))
		keyDataType = dataType.subtypes[0]
		valDataType = dataType.subtypes[1]
	}

	keys := rVal.MapKeys()
	for _, key := range keys {
		tv, err := newCassTypedVal(key.Interface(), keyDataType)
		if err != nil {
			return nil, err
		}
		defer tv.Free()
		if keyDataType.Equals(CUnknown) {
			keyDataType = tv.Kind()
		}
		if err = tv.BindTo(ctv, -1); err != nil {
			return nil, err
		}
		tv, err = newCassTypedVal(rVal.MapIndex(key).Interface(), valDataType)
		if err != nil {
			return nil, err
		}
		defer tv.Free()
		if valDataType.Equals(CUnknown) {
			valDataType = tv.Kind()
			ctv.kind = ctv.kind.Specialize(keyDataType, valDataType)
		}
		if err = tv.BindTo(ctv, -1); err != nil {
			return nil, err
		}
	}

	return ctv, nil
}

func toSet(value interface{}, dataType CassType) (*collectionTypedVal, error) {
	switch value := value.(type) {
	case setmarker:
		return toSet(value.value, dataType)
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Slice, reflect.Array:
		col := C.cass_collection_new(C.CASS_COLLECTION_TYPE_SET, C.size_t(rVal.Len()))
		ctv := &collectionTypedVal{col, dataType, CASS_VALUE_TYPE_SET}

		// var elemDataType cassDataType = nil
		// if dataType != nil {
		// 	elemDataType = cassDataType(C.cass_data_type_sub_data_type(dataType, 0))
		// }
		elemDataType := CUnknown
		if len(dataType.subtypes) > 0 {
			elemDataType = dataType.subtypes[0]
		}

		idx := 0
		for idx < rVal.Len() {
			tv, err := newCassTypedVal(rVal.Index(idx).Interface(), elemDataType)
			if err != nil {
				return nil, err
			}
			defer tv.Free()
			if elemDataType.Equals(CUnknown) {
				elemDataType = tv.Kind()
				ctv.kind = ctv.kind.Specialize(elemDataType)
			}
			if err = tv.BindTo(ctv, -1); err != nil {
				return nil, err
			}
			idx += 1
		}

		return ctv, nil
	case reflect.Map:
		col := C.cass_collection_new(C.CASS_COLLECTION_TYPE_SET, C.size_t(rVal.Len()))
		ctv := &collectionTypedVal{col, dataType, CASS_VALUE_TYPE_SET}

		// var elemDataType cassDataType = nil
		// if dataType != nil {
		// 	elemDataType = cassDataType(C.cass_data_type_sub_data_type(dataType, 0))
		// }
		elemDataType := CUnknown
		if len(dataType.subtypes) > 0 {
			elemDataType = dataType.subtypes[0]
		}

		keys := rVal.MapKeys()
		for _, key := range keys {
			tv, err := newCassTypedVal(key.Interface(), elemDataType)
			if err != nil {
				return nil, err
			}
			defer tv.Free()
			if elemDataType.Equals(CUnknown) {
				elemDataType = tv.Kind()
				ctv.kind = ctv.kind.Specialize(elemDataType)
			}
			if err = tv.BindTo(ctv, -1); err != nil {
				return nil, err
			}
		}

		return ctv, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, dataType.String())
}

// implements internal `typedValue` interface
func (ctv collectionTypedVal) BindTo(dst interface{}, index int) error {
	var retc C.CassError
	pos := C.size_t(index)
	switch dst := dst.(type) {
	case *Statement:
		retc = C.cass_statement_bind_collection(dst.cptr, pos, ctv.cptr)
	case *tupleTypedVal:
		retc = C.cass_tuple_set_collection(dst.cptr, pos, ctv.cptr)
	}
	if retc != C.CASS_OK {
		return newError(retc)

	}
	return nil
}

func (ctv collectionTypedVal) Kind() CassType {
	return ctv.kind
}

func (ctv collectionTypedVal) Free() {
	C.cass_collection_free(ctv.cptr)
}

func (ttv tupleTypedVal) BindTo(dst interface{}, index int) error {
	var retc C.CassError
	pos := C.size_t(index)
	switch dst := dst.(type) {
	case *Statement:
		retc = C.cass_statement_bind_tuple(dst.cptr, pos, ttv.cptr)
	case *collectionTypedVal:
		retc = C.cass_collection_append_tuple(dst.cptr, ttv.cptr)
	case *tupleTypedVal:
		retc = C.cass_tuple_set_tuple(dst.cptr, pos, ttv.cptr)
	}
	if retc != C.CASS_OK {
		return newError(retc)
	}

	return nil
}

func (ttv tupleTypedVal) Kind() CassType {
	return ttv.kind
}

func (ttv tupleTypedVal) Free() {
	C.cass_tuple_free(ttv.cptr)
}

// implements internal `typedValue` interface
func (ptv primitiveTypedVal) BindTo(dst interface{}, index int) error {
	var retc C.CassError
	pos := C.size_t(index)

	switch ptv.kind.primary {
	case CASS_VALUE_TYPE_ASCII, CASS_VALUE_TYPE_TEXT, CASS_VALUE_TYPE_VARCHAR:
		cstr := C.CString(ptv.val.(string))
		defer C.free(unsafe.Pointer(cstr))

		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_string(dst.cptr, pos, cstr)
		case *collectionTypedVal:
			retc = C.cass_collection_append_string(dst.cptr, cstr)
		case *tupleTypedVal:
			retc = C.cass_tuple_set_string(dst.cptr, pos, cstr)
		}
	case CASS_VALUE_TYPE_BOOLEAN:
		val := C.cass_bool_t(ptv.val.(int))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_bool(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_bool(dst.cptr, val)
		case *tupleTypedVal:
			retc = C.cass_tuple_set_bool(dst.cptr, pos, val)
		}
		// int types (not yet VARINT)
	case CASS_VALUE_TYPE_BIGINT, CASS_VALUE_TYPE_TIMESTAMP, CASS_VALUE_TYPE_TIME:
		val := C.cass_int64_t(ptv.val.(int64))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_int64(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_int64(dst.cptr, val)
		case *tupleTypedVal:
			retc = C.cass_tuple_set_int64(dst.cptr, pos, val)
		}
	case CASS_VALUE_TYPE_INT:
		var ival C.cass_int32_t
		switch val := ptv.val.(type) {
		case int32:
			ival = C.cass_int32_t(val)
		case int:
			ival = C.cass_int32_t(val)
		default:
			panic(fmt.Sprintf("expecting int/int32 found %T", ptv.val))
		}
		// fmt.Printf("BindTo(%d) %T(%d): %d\n", index, ptv.val, ptv.val, ival)
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_int32(dst.cptr, pos, ival)
		case *collectionTypedVal:
			retc = C.cass_collection_append_int32(dst.cptr, ival)
		case *tupleTypedVal:
			retc = C.cass_tuple_set_int32(dst.cptr, pos, ival)
		}
	case CASS_VALUE_TYPE_SMALL_INT:
		val := C.cass_int16_t(ptv.val.(int16))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_int16(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_int16(dst.cptr, val)
		case *tupleTypedVal:
			retc = C.cass_tuple_set_int16(dst.cptr, pos, val)
		}
	case CASS_VALUE_TYPE_TINY_INT:
		val := C.cass_int8_t(ptv.val.(int8))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_int8(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_int8(dst.cptr, val)
		case *tupleTypedVal:
			retc = C.cass_tuple_set_int8(dst.cptr, pos, val)
		}
	// float types (not yet DECIMAL)
	case CASS_VALUE_TYPE_FLOAT:
		val := C.cass_float_t(ptv.val.(float32))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_float(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_float(dst.cptr, val)
		case *tupleTypedVal:
			retc = C.cass_tuple_set_float(dst.cptr, pos, val)
		}
	case CASS_VALUE_TYPE_DOUBLE:
		val := C.cass_double_t(ptv.val.(float64))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_double(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_double(dst.cptr, val)
		case *tupleTypedVal:
			retc = C.cass_tuple_set_double(dst.cptr, pos, val)
		}
	case CASS_VALUE_TYPE_DECIMAL:
		val := ptv.val.(*Decimal)
		buf := export2Complement(val.Value)
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_decimal(dst.cptr, pos,
				(*C.cass_byte_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)),
				C.cass_int32_t(val.Scale))
		case *collectionTypedVal:
			retc = C.cass_collection_append_decimal(dst.cptr,
				(*C.cass_byte_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)),
				C.cass_int32_t(val.Scale))
		case *tupleTypedVal:
			retc = C.cass_tuple_set_decimal(dst.cptr, pos,
				(*C.cass_byte_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)),
				C.cass_int32_t(val.Scale))

		}
	case CASS_VALUE_TYPE_UUID, CASS_VALUE_TYPE_TIMEUUID:
		cStr := C.CString(ptv.val.(UUID).String())
		defer C.free(unsafe.Pointer(cStr))
		var cUuid C.CassUuid
		retc = C.cass_uuid_from_string(cStr, &cUuid)
		if retc == C.CASS_OK {
			switch dst := dst.(type) {
			case *Statement:
				retc = C.cass_statement_bind_uuid(dst.cptr, pos, cUuid)
			case *collectionTypedVal:
				retc = C.cass_collection_append_uuid(dst.cptr, cUuid)
			case *tupleTypedVal:
				retc = C.cass_tuple_set_uuid(dst.cptr, pos, cUuid)
			}
		}
	case CASS_VALUE_TYPE_DATE:
		val := C.cass_uint32_t(ptv.val.(uint32))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_uint32(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_uint32(dst.cptr, val)
		case *tupleTypedVal:
			retc = C.cass_tuple_set_uint32(dst.cptr, pos, val)
		}
	case CASS_VALUE_TYPE_INET:
		val := ptv.val.([]byte)
		var cInet C.struct_CassInet_
		cInet.address_length = C.cass_uint8_t(len(val))
		for j, _ := range val {
			cInet.address[j] = C.cass_uint8_t(val[j])
		}
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_inet(dst.cptr, pos, cInet)
		case *collectionTypedVal:
			retc = C.cass_collection_append_inet(dst.cptr, cInet)
		case *tupleTypedVal:
			retc = C.cass_tuple_set_inet(dst.cptr, pos, cInet)
		}
	case CASS_VALUE_TYPE_BLOB, CASS_VALUE_TYPE_VARINT:
		val := ptv.val.([]byte)
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_bytes(dst.cptr, pos,
				(*C.cass_byte_t)(unsafe.Pointer(&val[0])), C.size_t(len(val)))
		case *collectionTypedVal:
			retc = C.cass_collection_append_bytes(dst.cptr,
				(*C.cass_byte_t)(unsafe.Pointer(&val[0])), C.size_t(len(val)))
		case *tupleTypedVal:
			retc = C.cass_tuple_set_bytes(dst.cptr, pos,
				(*C.cass_byte_t)(unsafe.Pointer(&val[0])), C.size_t(len(val)))
		}
	}

	if retc != C.CASS_OK {
		return newError(retc)

	}
	return nil
}

func (ptv primitiveTypedVal) Kind() CassType {
	return ptv.kind
}

func (ptv primitiveTypedVal) Free() {}

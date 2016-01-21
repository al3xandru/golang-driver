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

const maxUint = ^uint(0)
const maxInt = int(maxUint >> 1)

type primitiveTypedVal struct {
	val      interface{}
	cassType C.CassValueType
}

type collectionTypedVal struct {
	cptr     *C.struct_CassCollection_
	dataType cassDataType
	cassType C.CassValueType
}

type binder interface {
	BindTo(dst interface{}, index int) error
}

func write(stmt *Statement, value interface{}, index int, dataType cassDataType) error {
	// fmt.Printf("write(%v, %d, %d)\n", value, index, valueType(dataType))
	if value == nil {
		if retc := C.cass_statement_bind_null(stmt.cptr, C.size_t(index)); retc != C.CASS_OK {
			return newError(retc)
		}
	}
	tv, err := newCassTypedVal(value, dataType)
	if err != nil {
		return err
	}
	return tv.BindTo(stmt, index)
}

func newCassTypedVal(value interface{}, dataType cassDataType) (binder, error) {
	cassValueType := valueType(dataType)
	// if cassValueType != CASS_VALUE_TYPE_UNKNOWN {
	// 	fmt.Printf("CassValueType: %04x\n", cassValueType)
	// }

	switch cassValueType {
	case CASS_VALUE_TYPE_ASCII, CASS_VALUE_TYPE_TEXT, CASS_VALUE_TYPE_VARCHAR:
		return toText(value, cassValueType)
	case CASS_VALUE_TYPE_BOOLEAN:
		return toBool(value, cassValueType)
	case CASS_VALUE_TYPE_BIGINT:
		return toBigint(value, cassValueType)
	case CASS_VALUE_TYPE_INT:
		return toInt(value, cassValueType)
	case CASS_VALUE_TYPE_SMALL_INT:
		return toSmallInt(value, cassValueType)
	case CASS_VALUE_TYPE_TINY_INT:
		return toTinyInt(value, cassValueType)
	// case CASS_VALUE_TYPE_COUNTER, ,
	// 	return readInt(value, cassType, dst)
	case CASS_VALUE_TYPE_VARINT:
		return toVarint(value, cassValueType)
	case CASS_VALUE_TYPE_FLOAT:
		return toFloat(value, cassValueType)
	case CASS_VALUE_TYPE_DOUBLE:
		return toDouble(value, cassValueType)
	case CASS_VALUE_TYPE_DECIMAL:
		return toDecimal(value, cassValueType)
	case CASS_VALUE_TYPE_TIMESTAMP:
		return toTimestamp(value, cassValueType)
	case CASS_VALUE_TYPE_DATE:
		return toDate(value, cassValueType)
	case CASS_VALUE_TYPE_TIME:
		return toTime(value, cassValueType)
	case CASS_VALUE_TYPE_UUID, CASS_VALUE_TYPE_TIMEUUID:
		return toUUID(value, cassValueType)
	case CASS_VALUE_TYPE_INET:
		return toInet(value, cassValueType)
	case CASS_VALUE_TYPE_BLOB:
		return toBlob(value, cassValueType)
	case CASS_VALUE_TYPE_LIST:
		return toList(value, dataType)
	case CASS_VALUE_TYPE_SET:
		return toSet(value, dataType)
	case CASS_VALUE_TYPE_MAP:
		return toMap(value, dataType)
		// case CASS_VALUE_TYPE_TUPLE:
		// 	return readTuple(value, cassType, dst)
		// case CASS_VALUE_TYPE_UDT:
		// 	return readUDT(value, cassType, dst)
	}

	switch value := value.(type) {
	case bool:
		return toBool(value, CASS_VALUE_TYPE_BOOLEAN)
	case int64:
		return toBigint(value, CASS_VALUE_TYPE_BIGINT)
	case int32:
		return toInt(value, CASS_VALUE_TYPE_INT)
	case int16:
		return toSmallInt(value, CASS_VALUE_TYPE_SMALL_INT)
	case int8:
		return toTinyInt(value, CASS_VALUE_TYPE_TINY_INT)
	case int:
		// must determine if it's 64 or 32
		if value < math.MinInt32 || value > math.MaxInt32 {
			return toBigint(value, CASS_VALUE_TYPE_BIGINT)
		} else {
			return toInt(value, CASS_VALUE_TYPE_INT)
		}
	case *big.Int:
		return toVarint(value, CASS_VALUE_TYPE_VARINT)
	case float32:
		return toFloat(value, CASS_VALUE_TYPE_FLOAT)
	case float64:
		return toDouble(value, CASS_VALUE_TYPE_DOUBLE)
	case *Decimal:
		return toDecimal(value, CASS_VALUE_TYPE_DECIMAL)
	case string:
		return toText(value, CASS_VALUE_TYPE_TEXT)
	case UUID:
		switch value.Version() {
		case 1:
			return toUUID(value, CASS_VALUE_TYPE_TIMEUUID)
		case 4:
			return toUUID(value, CASS_VALUE_TYPE_UUID)
		}
	case Date:
		return toDate(value, CASS_VALUE_TYPE_DATE)
	case Time:
		return toTime(value, CASS_VALUE_TYPE_TIME)
	case Timestamp:
		return toTimestamp(value, CASS_VALUE_TYPE_TIMESTAMP)
	case net.IP:
		return toInet(value, CASS_VALUE_TYPE_INET)
	case []byte:
		return toBlob(value, CASS_VALUE_TYPE_BLOB)
	case *internalSet:
		return toSet(value.value, nil)
	}
	// last attempt
	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Bool:
		return toBool(value, CASS_VALUE_TYPE_BOOLEAN)
	case reflect.Int64:
		return toBigint(value, CASS_VALUE_TYPE_BIGINT)
	case reflect.Int32:
		return toInt(value, CASS_VALUE_TYPE_INT)
	case reflect.Int16:
		return toSmallInt(value, CASS_VALUE_TYPE_SMALL_INT)
	case reflect.Int8:
		return toTinyInt(value, CASS_VALUE_TYPE_TINY_INT)
	case reflect.Int:
		// if maxInt > 2147483647 {
		if rVal.Int() < math.MinInt32 || rVal.Int() > math.MaxInt32 {
			return toBigint(rVal.Int(), CASS_VALUE_TYPE_BIGINT)
		} else {
			return toInt(rVal.Int(), CASS_VALUE_TYPE_INT)
		}
	case reflect.Float32:
		return toFloat(value, CASS_VALUE_TYPE_FLOAT)
	case reflect.Float64:
		return toDouble(value, CASS_VALUE_TYPE_DOUBLE)
	case reflect.String:
		return toText(value, CASS_VALUE_TYPE_TEXT)
	case reflect.Map:
		return toMap(value, nil)
	case reflect.Slice, reflect.Array:
		return toList(value, nil)
	}

	return nil, fmt.Errorf("unknown type %T", value)
}

func toBool(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case bool:
		if value {
			return &primitiveTypedVal{1, CASS_VALUE_TYPE_BOOLEAN}, nil
		} else {
			return &primitiveTypedVal{0, CASS_VALUE_TYPE_BOOLEAN}, nil
		}
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Bool:
		if rVal.Bool() {
			return &primitiveTypedVal{1, CASS_VALUE_TYPE_BOOLEAN}, nil
		} else {
			return &primitiveTypedVal{0, CASS_VALUE_TYPE_BOOLEAN}, nil
		}
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toBigint(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case int64:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_BIGINT}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int64:
		return &primitiveTypedVal{rVal.Int(), CASS_VALUE_TYPE_BIGINT}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toInt(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case int32, int:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_INT}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int32, reflect.Int:
		return &primitiveTypedVal{rVal.Int(), CASS_VALUE_TYPE_INT}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toSmallInt(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case int16:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_SMALL_INT}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int16:
		return &primitiveTypedVal{rVal.Int(), CASS_VALUE_TYPE_SMALL_INT}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toTinyInt(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case int8:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_TINY_INT}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int8:
		return &primitiveTypedVal{rVal.Int(), CASS_VALUE_TYPE_TINY_INT}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toVarint(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case *big.Int:
		buf := export2Complement(value)
		return &primitiveTypedVal{buf, CASS_VALUE_TYPE_VARINT}, nil
	}
	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toFloat(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case float32:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_FLOAT}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Float32:
		return &primitiveTypedVal{rVal.Float(), CASS_VALUE_TYPE_DOUBLE}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toDouble(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case float64:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_DOUBLE}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Float64:
		return &primitiveTypedVal{rVal.Float(), CASS_VALUE_TYPE_DOUBLE}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toDecimal(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case *Decimal:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_DECIMAL}, nil
	}
	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

// Could allow string too
func toUUID(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	if value, ok := value.(UUID); ok {
		return &primitiveTypedVal{value, cassType}, nil
	}
	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toText(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case string:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_TEXT}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.String:
		return &primitiveTypedVal{rVal.String(), CASS_VALUE_TYPE_TEXT}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toDate(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case Date:
		return &primitiveTypedVal{value.Days, CASS_VALUE_TYPE_DATE}, nil
	case uint32:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_DATE}, nil
	case string:
		date, err := ParseDate(value)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %T (%s) into %s",
				value, value, cassTypeName(cassType))
		}
		return &primitiveTypedVal{date.Days, CASS_VALUE_TYPE_DATE}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Uint32:
		return &primitiveTypedVal{rVal.Uint(), CASS_VALUE_TYPE_DATE}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toTime(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case Time:
		return &primitiveTypedVal{int64(value), CASS_VALUE_TYPE_TIME}, nil
	case int64:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_TIME}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int64:
		return &primitiveTypedVal{rVal.Int(), CASS_VALUE_TYPE_TIME}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toTimestamp(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case Timestamp:
		return &primitiveTypedVal{value.SecondsSinceEpoch, CASS_VALUE_TYPE_TIMESTAMP}, nil
	case int64:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_TIMESTAMP}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Int64:
		return &primitiveTypedVal{rVal.Int(), CASS_VALUE_TYPE_TIME}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toInet(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case net.IP:
		return &primitiveTypedVal{[]byte(value), CASS_VALUE_TYPE_INET}, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toBlob(value interface{}, cassType C.CassValueType) (*primitiveTypedVal, error) {
	switch value := value.(type) {
	case []byte:
		return &primitiveTypedVal{value, CASS_VALUE_TYPE_BLOB}, nil
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Slice, reflect.Array:
		if rVal.Type().Elem().Kind() == reflect.Uint8 {
			return &primitiveTypedVal{rVal.Bytes(), CASS_VALUE_TYPE_BLOB}, nil
		}
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value, cassTypeName(cassType))
}

func toList(value interface{}, dataType cassDataType) (*collectionTypedVal, error) {
	// fmt.Printf("toList(%v)\n", value)
	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Slice, reflect.Array:
		col := C.cass_collection_new(C.CASS_COLLECTION_TYPE_LIST, C.size_t(rVal.Len()))
		ctv := &collectionTypedVal{col, dataType, CASS_VALUE_TYPE_LIST}

		var elemDataType cassDataType = nil
		if dataType != nil {
			elemDataType = cassDataType(C.cass_data_type_sub_data_type(dataType, 0))
		}

		idx := 0
		for idx < rVal.Len() {
			tv, err := newCassTypedVal(rVal.Index(idx).Interface(), elemDataType)
			if err != nil {
				return nil, err
			}
			if err = tv.BindTo(ctv, -1); err != nil {
				return nil, err
			}
			idx += 1
		}
		return ctv, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value,
		collectionTypeName(dataType, CASS_VALUE_TYPE_LIST))
}

func toMap(value interface{}, dataType cassDataType) (*collectionTypedVal, error) {
	rVal := reflect.ValueOf(value)
	if rVal.Type().Kind() != reflect.Map {
		return nil, fmt.Errorf("cannot convert %T into %s", value,
			collectionTypeName(dataType, CASS_VALUE_TYPE_MAP))
	}
	col := C.cass_collection_new(C.CASS_COLLECTION_TYPE_MAP, C.size_t(rVal.Len()))
	ctv := &collectionTypedVal{col, dataType, CASS_VALUE_TYPE_MAP}

	var keyDataType, valDataType cassDataType = nil, nil
	if dataType != nil {
		keyDataType = cassDataType(C.cass_data_type_sub_data_type(dataType, 0))
		valDataType = cassDataType(C.cass_data_type_sub_data_type(dataType, 1))
	}

	keys := rVal.MapKeys()
	for _, key := range keys {
		tv, err := newCassTypedVal(key.Interface(), keyDataType)
		if err != nil {
			return nil, err
		}
		if err = tv.BindTo(ctv, -1); err != nil {
			return nil, err
		}
		tv, err = newCassTypedVal(rVal.MapIndex(key).Interface(), valDataType)
		if err = tv.BindTo(ctv, -1); err != nil {
			return nil, err
		}
	}

	return ctv, nil
}

func toSet(value interface{}, dataType cassDataType) (*collectionTypedVal, error) {
	switch value := value.(type) {
	case *internalSet:
		return toSet(value.value, dataType)
	}

	rVal := reflect.ValueOf(value)
	switch rVal.Type().Kind() {
	case reflect.Slice, reflect.Array:
		col := C.cass_collection_new(C.CASS_COLLECTION_TYPE_SET, C.size_t(rVal.Len()))
		ctv := &collectionTypedVal{col, dataType, CASS_VALUE_TYPE_SET}

		var elemDataType cassDataType = nil
		if dataType != nil {
			elemDataType = cassDataType(C.cass_data_type_sub_data_type(dataType, 0))
		}

		idx := 0
		for idx < rVal.Len() {
			tv, err := newCassTypedVal(rVal.Index(idx).Interface(), elemDataType)
			if err != nil {
				return nil, err
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

		var elemDataType cassDataType = nil
		if dataType != nil {
			elemDataType = cassDataType(C.cass_data_type_sub_data_type(dataType, 0))
		}

		keys := rVal.MapKeys()
		for _, key := range keys {
			tv, err := newCassTypedVal(key.Interface(), elemDataType)
			if err != nil {
				return nil, err
			}
			if err = tv.BindTo(ctv, -1); err != nil {
				return nil, err
			}
		}

		return ctv, nil
	}

	return nil, fmt.Errorf("cannot convert %T into %s", value,
		collectionTypeName(dataType, CASS_VALUE_TYPE_SET))
}

func collectionTypeName(dataType cassDataType, valType C.CassValueType) string {
	if dataType == nil {
		return cassTypeName(valType)
	}
	switch valType {
	case CASS_VALUE_TYPE_LIST:
		return fmt.Sprintf("%s<%s>", cassTypeName(valType),
			cassTypeName(valueType(cassDataType(C.cass_data_type_sub_data_type(dataType, 0)))))
	case CASS_VALUE_TYPE_SET:
		return fmt.Sprintf("%s<%s>", cassTypeName(valType),
			cassTypeName(valueType(cassDataType(C.cass_data_type_sub_data_type(dataType, 0)))))
	case CASS_VALUE_TYPE_MAP:
		return fmt.Sprintf("%s<%s, %s>", cassTypeName(valType),
			cassTypeName(valueType(cassDataType(C.cass_data_type_sub_data_type(dataType, 0)))),
			cassTypeName(valueType(cassDataType(C.cass_data_type_sub_data_type(dataType, 1)))))
	}
	return cassTypeName(valType)
}

// implements internal `binder` interface
func (ctv *collectionTypedVal) BindTo(dst interface{}, index int) error {
	var retc C.CassError
	switch dst := dst.(type) {
	case *Statement:
		retc = C.cass_statement_bind_collection(dst.cptr, C.size_t(index), ctv.cptr)
	}
	if retc != C.CASS_OK {
		return newError(retc)

	}
	return nil
}

// implements internal `binder` interface
func (ptv *primitiveTypedVal) BindTo(dst interface{}, index int) error {
	var retc C.CassError
	pos := C.size_t(index)

	switch ptv.cassType {
	case CASS_VALUE_TYPE_TEXT:
		cstr := C.CString(ptv.val.(string))
		defer C.free(unsafe.Pointer(cstr))

		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_string(dst.cptr, pos, cstr)
		case *collectionTypedVal:
			retc = C.cass_collection_append_string(dst.cptr, cstr)
		}
	case CASS_VALUE_TYPE_BOOLEAN:
		val := C.cass_bool_t(ptv.val.(int))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_bool(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_bool(dst.cptr, val)
		}
		// int types (not yet VARINT)
	case CASS_VALUE_TYPE_BIGINT, CASS_VALUE_TYPE_TIMESTAMP, CASS_VALUE_TYPE_TIME:
		val := C.cass_int64_t(ptv.val.(int64))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_int64(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_int64(dst.cptr, val)
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
		}
	case CASS_VALUE_TYPE_SMALL_INT:
		val := C.cass_int16_t(ptv.val.(int16))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_int16(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_int16(dst.cptr, val)
		}
	case CASS_VALUE_TYPE_TINY_INT:
		val := C.cass_int8_t(ptv.val.(int8))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_int8(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_int8(dst.cptr, val)
		}
	// float types (not yet DECIMAL)
	case CASS_VALUE_TYPE_FLOAT:
		val := C.cass_float_t(ptv.val.(float32))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_float(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_float(dst.cptr, val)
		}
	case CASS_VALUE_TYPE_DOUBLE:
		val := C.cass_double_t(ptv.val.(float64))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_double(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_double(dst.cptr, val)
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
			}
		}
	case CASS_VALUE_TYPE_DATE:
		val := C.cass_uint32_t(ptv.val.(uint32))
		switch dst := dst.(type) {
		case *Statement:
			retc = C.cass_statement_bind_uint32(dst.cptr, pos, val)
		case *collectionTypedVal:
			retc = C.cass_collection_append_uint32(dst.cptr, val)
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
		}
	}

	if retc != C.CASS_OK {
		return newError(retc)

	}
	return nil
}

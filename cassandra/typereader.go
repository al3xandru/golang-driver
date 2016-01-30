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
	"net"
	"reflect"
	"unsafe"
)

func read(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	// fmt.Printf("CVT1: 0x%04X\n", cassType)
	if isNull(value) && canBeNil(dst) {
		dstVal := reflect.ValueOf(dst)
		nilVal := reflect.Zero(dstVal.Type().Elem())
		dstVal.Elem().Set(nilVal)
		return false, nil
	}

	switch cassType.PrimaryType {
	case CASS_VALUE_TYPE_ASCII, CASS_VALUE_TYPE_TEXT, CASS_VALUE_TYPE_VARCHAR:
		return readText(value, cassType, dst)
	case CASS_VALUE_TYPE_BIGINT, CASS_VALUE_TYPE_COUNTER, CASS_VALUE_TYPE_INT,
		CASS_VALUE_TYPE_SMALL_INT, CASS_VALUE_TYPE_TINY_INT:
		return readInt(value, cassType, dst)
	case CASS_VALUE_TYPE_VARINT:
		return readVarint(value, cassType, dst)
	case CASS_VALUE_TYPE_BLOB:
		return readBlob(value, cassType, dst)
	case CASS_VALUE_TYPE_BOOLEAN:
		return readBool(value, cassType, dst)
	case CASS_VALUE_TYPE_DECIMAL:
		return readDecimal(value, cassType, dst)
	case CASS_VALUE_TYPE_DOUBLE:
		return readDouble(value, cassType, dst)
	case CASS_VALUE_TYPE_FLOAT:
		return readFloat(value, cassType, dst)
	case CASS_VALUE_TYPE_TIMESTAMP:
		return readTimestamp(value, cassType, dst)
	case CASS_VALUE_TYPE_DATE:
		return readDate(value, cassType, dst)
	case CASS_VALUE_TYPE_TIME:
		return readTime(value, cassType, dst)
	case CASS_VALUE_TYPE_UUID, CASS_VALUE_TYPE_TIMEUUID:
		return readUUID(value, cassType, dst)
	case CASS_VALUE_TYPE_INET:
		return readInet(value, cassType, dst)
	case CASS_VALUE_TYPE_LIST:
		return readList(value, cassType, dst)
	case CASS_VALUE_TYPE_SET:
		return readSet(value, cassType, dst)
	case CASS_VALUE_TYPE_MAP:
		return readMap(value, cassType, dst)
	case CASS_VALUE_TYPE_TUPLE:
		return readTuple(value, cassType, dst)
	case CASS_VALUE_TYPE_UDT:
		return readUDT(value, cassType, dst)
	}
	return true, fmt.Errorf("unknown type %s to read into %T",
		cassType.Name(), dst)
}

func readBlob(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *[]byte:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsBlob(value)
		*dst = v
		return f, err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}
	dstVal = dstVal.Elem()
	if dstVal.Type().Kind() == reflect.Slice && dstVal.Type().Elem().Kind() == reflect.Uint8 {
		f, v, err := valAsBlob(value)
		dstVal.SetBytes(v)
		return f, err
	}

	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func valAsBlob(value *C.CassValue) (found bool, b []byte, err error) {
	var buf *C.cass_byte_t
	var sz C.size_t
	retc := C.cass_value_get_bytes(value, &buf, &sz)
	switch retc {
	case C.CASS_OK:
		b = C.GoBytes(unsafe.Pointer(buf), C.int(sz))
		found = true
	case C.CASS_ERROR_LIB_NULL_VALUE:
		found = false
		return
	default:
		err = newError(retc)
	}
	return
}

func readBool(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *bool:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsBool(value)
		*dst = v
		return f, err
	}
	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}
	dstVal = dstVal.Elem()
	switch dstVal.Type().Kind() {
	case reflect.Bool:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsBool(value)
		dstVal.SetBool(v)
		return f, err
	case reflect.Interface:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsBool(value)
		dstVal.Set(reflect.ValueOf(v))
		return f, err
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func valAsBool(value *C.CassValue) (found bool, b bool, err error) {
	var cb C.cass_bool_t
	retc := C.cass_value_get_bool(value, &cb)
	switch retc {
	case C.CASS_OK:
		b = cb != 0
		found = true
	case C.CASS_ERROR_LIB_NULL_VALUE:
		found = false
	default:
		err = newError(retc)
	}
	return
}

func readText(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *string:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsString(value)
		*dst = v
		return f, err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}
	dstVal = dstVal.Elem()
	switch dstVal.Type().Kind() {
	case reflect.String:
		if isNull(value) {
			dstVal.Set(reflect.Zero(dstVal.Type()))
			return false, nil
		}
		f, v, err := valAsString(value)
		dstVal.SetString(v)
		return f, err
	case reflect.Interface:
		if isNull(value) {
			dstVal.Set(reflect.Zero(reflect.TypeOf("")))
			return false, nil
		}
		f, v, err := valAsString(value)
		dstVal.Set(reflect.ValueOf(v))
		return f, err
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func valAsString(value *C.CassValue) (found bool, v string, err error) {
	var cstr *C.char
	var csize C.size_t
	retc := C.cass_value_get_string(value, &cstr, &csize)
	switch retc {
	case C.CASS_ERROR_LIB_NULL_VALUE:
		return
	case C.CASS_OK:
		v = C.GoStringN(cstr, C.int(csize))
		found = true
	default:
		err = newError(retc)
	}
	return
}

func readInt(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *int8:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		*dst = int8(v)
		return f, err
	case *int16:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		*dst = int16(v)
		return f, err
	case *int32:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		*dst = int32(v)
		return f, err
	case *int64:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		*dst = int64(v)
		return f, err
	case *int:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		*dst = int(v)
		return f, err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}

	dstVal = dstVal.Elem()

	switch dstVal.Type().Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		if isNull(value) {
			dstVal.Set(reflect.Zero(dstVal.Type()))
			return false, nil
		} else {
			f, v, err := valAsInt(value)
			dstVal.SetInt(v)
			return f, err
		}
	case reflect.Interface:
		if isNull(value) {
			dstVal.Set(reflect.ValueOf(0))
			return false, nil
		} else {
			f, v, err := valAsInt(value)
			dstVal.Set(reflect.ValueOf(v))
			return f, err
		}
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func valAsInt(value *C.CassValue) (found bool, v int64, err error) {
	cassType := C.cass_value_type(value)
	switch cassType {
	case CASS_VALUE_TYPE_TINY_INT:
		var ival C.cass_int8_t
		retc := C.cass_value_get_int8(value, &ival)
		switch retc {
		case C.CASS_OK:
			found = true
			v = int64(ival)
		case C.CASS_ERROR_LIB_NULL_VALUE:
			found = false
		default:
			found = true
			err = newError(retc)
		}
		return

	case CASS_VALUE_TYPE_SMALL_INT:
		var ival C.cass_int16_t
		retc := C.cass_value_get_int16(value, &ival)
		switch retc {
		case C.CASS_OK:
			found = true
			v = int64(ival)
		case C.CASS_ERROR_LIB_NULL_VALUE:
			found = false
		default:
			err = newError(retc)
		}
		return

	case CASS_VALUE_TYPE_INT:
		var ival C.cass_int32_t
		retc := C.cass_value_get_int32(value, &ival)
		switch retc {
		case C.CASS_OK:
			found = true
			v = int64(ival)
		case C.CASS_ERROR_LIB_NULL_VALUE:
			found = false
		default:
			err = newError(retc)
		}
		return

	case CASS_VALUE_TYPE_BIGINT, CASS_VALUE_TYPE_TIME, CASS_VALUE_TYPE_TIMESTAMP:
		var ival C.cass_int64_t
		retc := C.cass_value_get_int64(value, &ival)
		switch retc {
		case C.CASS_OK:
			found = true
			v = int64(ival)
		case C.CASS_ERROR_LIB_NULL_VALUE:
			found = false
		default:
			err = newError(retc)
		}
		return

	case CASS_VALUE_TYPE_DATE:
		var uval C.cass_uint32_t
		retc := C.cass_value_get_uint32(value, &uval)
		switch retc {
		case C.CASS_OK:
			found = true
			v = int64(uval)
		case C.CASS_ERROR_LIB_NULL_VALUE:
			found = false
		default:
			err = newError(retc)
		}
		return
	}

	return true, 0, fmt.Errorf("cannot read %s", cassTypeName(cassType))
}

func readVarint(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *big.Int:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsBlob(value)
		import2Complement(v, dst)
		return f, err
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func readDecimal(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *Decimal:
		if isNull(value) {
			return false, nil
		}

		var buf *C.cass_byte_t
		var sz C.size_t
		var sc C.cass_int32_t
		retc := C.cass_value_get_decimal(value, &buf, &sz, &sc)
		switch retc {
		case C.CASS_OK:
			b := C.GoBytes(unsafe.Pointer(buf), C.int(sz))
			bigint := big.NewInt(0)
			import2Complement(b, bigint)
			dst.Value = bigint
			dst.Scale = int32(sc)
			return true, nil
		case C.CASS_ERROR_LIB_NULL_VALUE:
			return false, nil
		default:
			return true, newError(retc)
		}
	}

	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func readFloat(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *float32:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsFloat(value)
		*dst = float32(v)
		return f, err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}

	dstVal = dstVal.Elem()

	switch dstVal.Type().Kind() {
	case reflect.Float32:
		if isNull(value) {
			dstVal.Set(reflect.Zero(dstVal.Type()))
			return false, nil
		} else {
			f, v, err := valAsFloat(value)
			dstVal.SetFloat(float64(v))
			return f, err
		}
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func readDouble(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *float64:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsFloat(value)
		*dst = v
		return f, err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}

	dstVal = dstVal.Elem()

	switch dstVal.Type().Kind() {
	case reflect.Float64:
		if isNull(value) {
			dstVal.Set(reflect.Zero(dstVal.Type()))
			return false, nil
		} else {
			f, v, err := valAsFloat(value)
			dstVal.SetFloat(v)
			return f, err
		}
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func valAsFloat(value *C.CassValue) (found bool, v float64, err error) {
	cassType := C.cass_value_type(value)
	switch cassType {
	case CASS_VALUE_TYPE_FLOAT:
		var f32 C.cass_float_t
		retc := C.cass_value_get_float(value, &f32)
		switch retc {
		case C.CASS_OK:
			found = true
			v = float64(f32)
		case C.CASS_ERROR_LIB_NULL_VALUE:
			found = false
		default:
			err = newError(retc)
		}
		return

	case CASS_VALUE_TYPE_DOUBLE:
		var f64 C.cass_double_t
		retc := C.cass_value_get_double(value, &f64)
		switch retc {
		case C.CASS_OK:
			found = true
			v = float64(f64)
		case C.CASS_ERROR_LIB_NULL_VALUE:
			found = false
		default:
			err = newError(retc)
		}
		return
	}
	return true, 0, fmt.Errorf("cannot read %s", cassTypeName(cassType))
}

func readUUID(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *UUID:
		if isNull(value) {
			return false, nil
		}
		var cuuid C.struct_CassUuid_
		retc := C.cass_value_get_uuid(value, &cuuid)
		switch retc {
		case C.CASS_OK:
			buf := (*C.char)(C.malloc(C.CASS_UUID_STRING_LENGTH))
			defer C.free(unsafe.Pointer(buf))

			C.cass_uuid_string(cuuid, buf)
			suuid := C.GoString(buf)

			uuid, err := ParseUUID(suuid)
			if err != nil {
				return true, err
			}
			*dst = uuid
			return true, nil
		default:
			return true, errors.New(C.GoString(C.cass_error_desc(retc)))
		}
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func readTime(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *Time:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		*dst = Time(v)
		return f, err
	case *int64:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		*dst = v
		return f, err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}

	dstVal = dstVal.Elem()

	switch dstVal.Type().Kind() {
	case reflect.Int64:
		if isNull(value) {
			dstVal.Set(reflect.Zero(dstVal.Type()))
			return false, nil
		} else {
			f, v, err := valAsInt(value)
			dstVal.SetInt(v)
			return f, err
		}
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func readDate(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *Date:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		dst.days = uint32(v)
		return f, err
	case *uint32:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		*dst = uint32(v)
		return f, err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}

	dstVal = dstVal.Elem()

	switch dstVal.Type().Kind() {
	case reflect.Uint32:
		if isNull(value) {
			dstVal.Set(reflect.Zero(dstVal.Type()))
			return false, nil
		} else {
			f, v, err := valAsInt(value)
			dstVal.SetUint(uint64(v))
			return f, err
		}
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func readTimestamp(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *Timestamp:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		dst.secondsSinceEpoch = v
		return f, err
	case *int64:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		*dst = v
		return f, err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}

	dstVal = dstVal.Elem()

	switch dstVal.Type().Kind() {
	case reflect.Int64:
		if isNull(value) {
			dstVal.Set(reflect.Zero(dstVal.Type()))
			return false, nil
		} else {
			f, v, err := valAsInt(value)
			dstVal.SetInt(v)
			return f, err
		}
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func readInet(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *net.IP:
		if isNull(value) {
			return false, nil
		}
		var inet C.struct_CassInet_
		retc := C.cass_value_get_inet(value, &inet)
		switch retc {
		case C.CASS_OK:
			sz := int(inet.address_length)
			ip := make([]byte, sz)
			for i := 0; i < sz; i++ {
				ip[i] = byte(inet.address[i])
			}
			*dst = net.IP(ip)
			return true, nil
		case C.CASS_ERROR_LIB_NULL_VALUE:
			return false, nil
		default:
			return true, errors.New(C.GoString(C.cass_error_desc(retc)))
		}
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func readList(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}
	dstVal = dstVal.Elem()
	switch dstVal.Type().Kind() {
	case reflect.Slice:
		if isNull(value) {
			dstVal.Set(reflect.Zero(dstVal.Type()))
			return false, nil
		}
		// sz := size(value)
		sz := int(C.cass_value_item_count(value))
		dstVal.Set(reflect.MakeSlice(dstVal.Type(), sz, sz))
		colIter := C.cass_iterator_from_collection(value)
		defer C.cass_iterator_free(colIter)

		for i := 0; i < sz; i++ {
			C.cass_iterator_next(colIter)
			colVal := C.cass_iterator_get_value(colIter)
			// colTyp := C.cass_value_type(colVal)
			elemCassType := CassTypeFromDataType(C.cass_value_data_type(colVal))
			if _, err := read(colVal, elemCassType, dstVal.Index(i).Addr().Interface()); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func size(value *C.CassValue) int {
	colIter := C.cass_iterator_from_collection(value)
	if colIter == nil {
		return -1
	}
	defer C.cass_iterator_free(colIter)
	count := 0
	for b := C.cass_iterator_next(colIter); b != 0; b = C.cass_iterator_next(colIter) {
		count++
	}
	return count
}

func readMap(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}
	dstVal = dstVal.Elem()
	if dstVal.Type().Kind() != reflect.Map {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}
	if isNull(value) {
		dstVal.Set(reflect.Zero(dstVal.Type()))
		return false, nil
	}
	t := dstVal.Type()
	dstVal.Set(reflect.MakeMap(t))
	colIter := C.cass_iterator_from_map(value)
	defer C.cass_iterator_free(colIter)

	b := C.cass_iterator_next(colIter)
	for b != 0 {
		key := reflect.New(t.Key())
		keyValue := C.cass_iterator_get_map_key(colIter)
		// keyType := C.cass_value_type(keyValue)
		keyType := CassTypeFromDataType(C.cass_value_data_type(keyValue))

		if _, err := read(keyValue, keyType, key.Interface()); err != nil {
			return true, err
		}

		val := reflect.New(t.Elem())
		valValue := C.cass_iterator_get_map_value(colIter)
		// valType := C.cass_value_type(valValue)
		valType := CassTypeFromDataType(C.cass_value_data_type(valValue))

		if _, err := read(valValue, valType, val.Interface()); err != nil {
			return true, err
		}

		dstVal.SetMapIndex(key.Elem(), val.Elem())
		b = C.cass_iterator_next(colIter)
	}
	return true, nil
}

func readSet(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}
	dstVal = dstVal.Elem()
	if dstVal.Type().Kind() != reflect.Map {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassType.Name(), dst)
	}
	if isNull(value) {
		dstVal.Set(reflect.Zero(dstVal.Type()))
		return false, nil
	}
	t := dstVal.Type()
	dstVal.Set(reflect.MakeMap(t))
	inSet := true
	trueVal := reflect.ValueOf(&inSet)

	colIter := C.cass_iterator_from_collection(value)
	defer C.cass_iterator_free(colIter)

	b := C.cass_iterator_next(colIter)
	for b != 0 {
		key := reflect.New(t.Key())
		keyValue := C.cass_iterator_get_value(colIter)
		// keyType := C.cass_value_type(keyValue)
		keyType := CassTypeFromDataType(C.cass_value_data_type(keyValue))

		if _, err := read(keyValue, keyType, key.Interface()); err != nil {
			return true, err
		}

		dstVal.SetMapIndex(key.Elem(), trueVal.Elem())
		b = C.cass_iterator_next(colIter)
	}
	return true, nil
}

func readTuple(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *Tuple:
		if isNull(value) {
			return false, nil
		}

		colIter := C.cass_iterator_from_tuple(value)
		defer C.cass_iterator_free(colIter)

		sz := int(C.cass_value_item_count(value))
		dst.values = make([]interface{}, sz)
		subtypes := make([]CassType, sz)

		b := C.cass_iterator_next(colIter)
		for i := 0; b != 0; i++ {
			val := C.cass_iterator_get_value(colIter)
			subtypes[i] = CassTypeFromDataType(C.cass_value_data_type(val))

			if _, err := read(val, subtypes[i], &dst.values[i]); err != nil {
				return true, err
			}
			b = C.cass_iterator_next(colIter)
		}
		dst.kind = CASS_TUPLE.Subtype(subtypes...)

		return true, nil
	}

	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func readUDT(value *C.CassValue, cassType CassType, dst interface{}) (bool, error) {
	return true, fmt.Errorf("cannot read %s type into %T", cassType.Name(),
		dst)
}

func isNull(value *C.CassValue) bool {
	return bool(C.cass_value_is_null(value) != 0)
}

func canBeNil(dst interface{}) bool {
	v := reflect.ValueOf(dst)
	r := v.Kind() == reflect.Ptr && v.Type().Elem().Kind() == reflect.Ptr
	// fmt.Printf("canBeNil(%s, kind: %s, etype: %s, ekind: %s): %t\n",
	// 	v.Type().String(),
	// 	v.Kind().String(),
	// 	v.Type().Elem().String(),
	// 	v.Type().Elem().Kind().String(),
	// 	r)
	return r
}

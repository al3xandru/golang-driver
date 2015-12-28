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

func read(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
	fmt.Printf("CVT1: 0x%04X\n", cassType)
	if isNull(value) && canBeNil(dst) {
		fmt.Printf("Null value\n")
		dstVal := reflect.ValueOf(dst)
		nilVal := reflect.Zero(dstVal.Type().Elem())
		dstVal.Elem().Set(nilVal)
		return false, nil
	}
	// cvt := C.cass_value_type(value)
	// fmt.Printf("CVT2: 0x%04X\n", cvt)
	// cassDataType := C.cass_value_data_type(value)
	// if cassDataType != nil {
	// 	cassValueType := C.cass_data_type_type(cassDataType)
	// 	fmt.Printf("CVT3: 0x%04X\n", cassValueType)
	// }
	switch cassType {
	case CASS_VALUE_TYPE_ASCII, CASS_VALUE_TYPE_TEXT, CASS_VALUE_TYPE_VARCHAR:
		return readText(value, cassType, dst)
	case CASS_VALUE_TYPE_BIGINT, CASS_VALUE_TYPE_COUNTER, CASS_VALUE_TYPE_INT,
		CASS_VALUE_TYPE_SMALL_INT, CASS_VALUE_TYPE_TINY_INT:
		return readInt(value, cassType, dst)
	case CASS_VALUE_TYPE_BOOLEAN:
		return readBool(value, cassType, dst)
	case CASS_VALUE_TYPE_FLOAT:
		return readFloat(value, cassType, dst)
	case CASS_VALUE_TYPE_DOUBLE:
		return readDouble(value, cassType, dst)
	case CASS_VALUE_TYPE_DATE:
		return readDate(value, cassType, dst)
	case CASS_VALUE_TYPE_TIME:
		return readTime(value, cassType, dst)
	case CASS_VALUE_TYPE_TIMESTAMP:
		return readTimestamp(value, cassType, dst)
	case CASS_VALUE_TYPE_LIST:
		return readList(value, cassType, dst)
	case CASS_VALUE_TYPE_SET:
		return readSet(value, cassType, dst)
	case CASS_VALUE_TYPE_MAP:
		return readMap(value, cassType, dst)
	}
	return true, fmt.Errorf("unknown type %s to read into %T",
		cassTypeName(cassType), dst)
}

func readBool(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
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
			cassTypeName(cassType), dst)
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
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassTypeName(cassType),
		dst)
}

func valAsBool(value *C.CassValue) (found bool, b bool, err error) {
	var cb C.cass_bool_t
	retc := C.cass_value_get_bool(value, &cb)
	switch retc {
	case C.CASS_OK:
		b = cb != 0
		found = true
	default:
		err = errors.New(C.GoString(C.cass_error_desc(retc)))
	}
	return
}

func readText(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
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
			cassTypeName(cassType), dst)
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
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassTypeName(cassType),
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
		return
	default:
		err = errors.New(C.GoString(C.cass_error_desc(retc)))
		return
	}

}

func readInt(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
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
			cassTypeName(cassType), dst)
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
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassTypeName(cassType),
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
			return true, int64(ival), nil
		default:
			return true, 0, errors.New(C.GoString(C.cass_error_desc(retc)))
		}

	case CASS_VALUE_TYPE_SMALL_INT:
		var ival C.cass_int16_t
		retc := C.cass_value_get_int16(value, &ival)
		switch retc {
		case C.CASS_OK:
			return true, int64(ival), nil
		default:
			return true, 0, errors.New(C.GoString(C.cass_error_desc(retc)))
		}

	case CASS_VALUE_TYPE_INT:
		var ival C.cass_int32_t
		retc := C.cass_value_get_int32(value, &ival)
		switch retc {
		case C.CASS_OK:
			return true, int64(ival), nil
		default:
			return true, 0, errors.New(C.GoString(C.cass_error_desc(retc)))
		}

	case CASS_VALUE_TYPE_BIGINT, CASS_VALUE_TYPE_TIME, CASS_VALUE_TYPE_TIMESTAMP:
		var ival C.cass_int64_t
		retc := C.cass_value_get_int64(value, &ival)
		switch retc {
		case C.CASS_OK:
			return true, int64(ival), nil
		default:
			return true, 0, errors.New(C.GoString(C.cass_error_desc(retc)))
		}
	case CASS_VALUE_TYPE_DATE:
		var uval C.cass_uint32_t
		retc := C.cass_value_get_uint32(value, &uval)
		switch retc {
		case C.CASS_OK:
			return true, int64(uval), nil
		default:
			return true, 0, errors.New(C.GoString(C.cass_error_desc(retc)))
		}
	}

	return true, 0, fmt.Errorf("cannot read %s", cassTypeName(cassType))
}

func readFloat(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
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
			cassTypeName(cassType), dst)
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
	return true, fmt.Errorf("cannot read %s type into %T", cassTypeName(cassType),
		dst)
}

func readDouble(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
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
			cassTypeName(cassType), dst)
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
	return true, fmt.Errorf("cannot read %s type into %T", cassTypeName(cassType),
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
			return
		default:
			return true, 0, errors.New(C.GoString(C.cass_error_desc(retc)))
		}
	case CASS_VALUE_TYPE_DOUBLE:
		var f64 C.cass_double_t
		retc := C.cass_value_get_double(value, &f64)
		switch retc {
		case C.CASS_OK:
			found = true
			v = float64(f64)
			return
		default:
			return true, 0, errors.New(C.GoString(C.cass_error_desc(retc)))
		}
	}
	return true, 0, fmt.Errorf("cannot read %s", cassTypeName(cassType))
}

func readTime(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
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
			cassTypeName(cassType), dst)
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
	return true, fmt.Errorf("cannot read %s type into %T", cassTypeName(cassType),
		dst)
}

func readDate(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *Date:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		dst.Days = uint32(v)
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
			cassTypeName(cassType), dst)
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
	return true, fmt.Errorf("cannot read %s type into %T", cassTypeName(cassType),
		dst)
}

func readTimestamp(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
	switch dst := dst.(type) {
	case *Timestamp:
		if isNull(value) {
			return false, nil
		}
		f, v, err := valAsInt(value)
		dst.SecondsSinceEpoch = v
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
			cassTypeName(cassType), dst)
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
	return true, fmt.Errorf("cannot read %s type into %T", cassTypeName(cassType),
		dst)
}

func readList(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassTypeName(cassType), dst)
	}
	dstVal = dstVal.Elem()
	switch dstVal.Type().Kind() {
	case reflect.Slice:
		if isNull(value) {
			dstVal.Set(reflect.Zero(dstVal.Type()))
			return false, nil
		}
		sz := size(value)
		dstVal.Set(reflect.MakeSlice(dstVal.Type(), sz, sz))
		colIter := C.cass_iterator_from_collection(value)
		defer C.cass_iterator_free(colIter)

		for i := 0; i < sz; i++ {
			C.cass_iterator_next(colIter)
			colVal := C.cass_iterator_get_value(colIter)
			colTyp := C.cass_value_type(colVal)
			if _, err := read(colVal, colTyp, dstVal.Index(i).Addr().Interface()); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	return true, fmt.Errorf("cannot read %s type into %T", cassTypeName(cassType),
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

func readMap(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassTypeName(cassType), dst)
	}
	dstVal = dstVal.Elem()
	if dstVal.Type().Kind() != reflect.Map {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassTypeName(cassType), dst)
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
		fmt.Printf("Key: %v, Val: %v\n", key, keyValue)
		keyType := C.cass_value_type(keyValue)

		if _, err := read(keyValue, keyType, key.Interface()); err != nil {
			return true, err
		}

		val := reflect.New(t.Elem())
		valValue := C.cass_iterator_get_map_value(colIter)
		valType := C.cass_value_type(valValue)

		if _, err := read(valValue, valType, val.Interface()); err != nil {
			return true, err
		}

		dstVal.SetMapIndex(key.Elem(), val.Elem())
		b = C.cass_iterator_next(colIter)
	}
	return true, nil
}

func readSet(value *C.CassValue, cassType C.CassValueType, dst interface{}) (bool, error) {
	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassTypeName(cassType), dst)
	}
	dstVal = dstVal.Elem()
	if dstVal.Type().Kind() != reflect.Map {
		return true, fmt.Errorf("cannot read %s into non-pointer %T",
			cassTypeName(cassType), dst)
	}
	if isNull(value) {
		dstVal.Set(reflect.Zero(dstVal.Type()))
		return false, nil
	}
	t := dstVal.Type()
	dstVal.Set(reflect.MakeMap(t))
	colIter := C.cass_iterator_from_collection(value)
	defer C.cass_iterator_free(colIter)

	b := C.cass_iterator_next(colIter)
	for b != 0 {
		key := reflect.New(t.Key())
		keyValue := C.cass_iterator_get_map_key(colIter)
		keyType := C.cass_value_type(keyValue)

		if _, err := read(keyValue, keyType, key.Interface()); err != nil {
			return true, err
		}

		val := reflect.New(t.Elem())
		valValue := C.cass_iterator_get_map_value(colIter)
		valType := C.cass_value_type(valValue)

		if _, err := read(valValue, valType, val.Interface()); err != nil {
			return true, err
		}

		dstVal.SetMapIndex(key.Elem(), val.Elem())
		b = C.cass_iterator_next(colIter)
	}
	return true, nil
}

func isNull(value *C.CassValue) bool {
	return bool(C.cass_value_is_null(value) != 0)
}

func canBeNil(dst interface{}) bool {
	v := reflect.ValueOf(dst)
	r := v.Kind() == reflect.Ptr && v.Type().Elem().Kind() == reflect.Ptr
	fmt.Printf("canBeNil(%s, kind: %s, etype: %s, ekind: %s): %t\n",
		v.Type().String(),
		v.Kind().String(),
		v.Type().Elem().String(),
		v.Type().Elem().Kind().String(),
		r)
	return r
}

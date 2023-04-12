package splunker

import (
	"reflect"
	"unsafe"
)

func readString(r *CountedReader, length uint64) (string, error) {
	b := make([]byte, length)
	_, err := r.Read(b)
	if err != nil {
		return "", err
	}
	return bytesToStr(b), nil
}

// bytesToStr creates a string pointing at the slice to avoid copying.
//
// Warning: the string returned by the function should be used with care, as the whole input data
// chunk may be either blocked from being freed by GC because of a single string or the buffer.Data
// may be garbage-collected even when the string exists.
func bytesToStr(data []byte) string {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	shdr := reflect.StringHeader{Data: h.Data, Len: h.Len}
	return *(*string)(unsafe.Pointer(&shdr))
}

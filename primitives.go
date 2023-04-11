package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"unsafe"
)

func readVariableWidthInt(r io.ByteReader) (int64, error) {
	uvarint, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	return int64(uvarint), nil
}

func readVariableWidthIntAsInt(r io.ByteReader) (int32, error) {
	val, err := readVariableWidthInt(r)
	if err != nil {
		return 0, fmt.Errorf("failed to read variable width int as int: %w", err)
	}
	if val>>31 != 0 {
		return 0, fmt.Errorf("deserialized number overflowed signed 4 bytes")
	}
	return int32(val), nil
}

func readVariableWidthLong(r io.ByteReader) (int64, error) {
	uvarint, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	return int64(uvarint), nil
}

func readVariableWidthSignedLong(r io.ByteReader) (int64, error) {
	uvarint, err := binary.ReadVarint(r)
	if err != nil {
		return 0, err
	}
	return int64(uvarint), nil
}

func readVariableWidthSignedInt(r io.ByteReader) (int32, error) {
	uvarint, err := binary.ReadVarint(r)
	if err != nil {
		return 0, err
	}
	return int32(uvarint), nil
}

func read64BitLong(r io.ByteReader) (int64, error) {
	return readFixedWidthNumber(r, 64)
}

func read32BitLong(r io.ByteReader) (int64, error) {
	return readFixedWidthNumber(r, 32)
}

func readFixedWidthNumber(r io.ByteReader, numBits int) (int64, error) {
	var scratchPad int64
	var shift int
	for shift < numBits {
		datum, err := r.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("failed to read fixed width number: %w", err)
		}
		byteAsLong := int64(0xFF & datum)
		scratchPad |= byteAsLong << shift
		shift += 8
	}
	return scratchPad, nil
}

func isBitSet(num, bitPos int32) bool {
	mask := int32(1) << bitPos
	return (num & mask) != 0
}

func readString(r *CountedReader, length int64) (string, error) {
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

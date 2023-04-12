// Package varint decodes varints
// found via https://www.dolthub.com/blog/2021-01-08-optimizing-varint-decoding/
package varint

func Varint(buf []byte) (int64, int) {
	ux, n := Uvarint(buf) // ok to continue in presence of error
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, n
}

func Uvarint(buf []byte) (uint64, int) {
	b := uint64(buf[0])
	if b < 0x80 {
		return b, 1
	}

	x := b & 0x7f
	b = uint64(buf[1])
	if b < 0x80 {
		return x | (b << 7), 2
	}

	x |= (b & 0x7f) << 7
	b = uint64(buf[2])
	if b < 0x80 {
		return x | (b << 14), 3
	}

	x |= (b & 0x7f) << 14
	b = uint64(buf[3])
	if b < 0x80 {
		return x | (b << 21), 4
	}

	x |= (b & 0x7f) << 21
	b = uint64(buf[4])
	if b < 0x80 {
		return x | (b << 28), 5
	}

	x |= (b & 0x7f) << 28
	b = uint64(buf[5])
	if b < 0x80 {
		return x | (b << 35), 6
	}

	x |= (b & 0x7f) << 35
	b = uint64(buf[6])
	if b < 0x80 {
		return x | (b << 42), 7
	}

	x |= (b & 0x7f) << 42
	b = uint64(buf[7])
	if b < 0x80 {
		return x | (b << 49), 8
	}

	x |= (b & 0x7f) << 49
	b = uint64(buf[8])
	if b < 0x80 {
		return x | (b << 56), 9
	}

	x |= (b & 0x7f) << 56
	b = uint64(buf[9])
	if b < 0x80 {
		return x | (b << 63), 10
	}

	return 0, -10
}

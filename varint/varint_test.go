package varint

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// copied from https://github.com/dolthub/dolt/blob/main/go/store/types/codec_test.go

type ve struct {
	val      uint64
	encoding []byte
}

func initToDecode(b *testing.B, numItems int) []ve {
	toDecode := make([]ve, b.N*numItems)

	r := rand.New(rand.NewSource(0))
	for i := 0; i < b.N*numItems; i++ {
		desiredSize := (i % 10) + 1
		min := uint64(0)
		max := uint64(0x80)

		if desiredSize < 10 {
			for j := 0; j < desiredSize-1; j++ {
				min = max
				max <<= 7
			}
		} else {
			min = 0x8000000000000000
			max = 0xffffffffffffffff
		}

		val := min + (r.Uint64() % (max - min))
		buf := make([]byte, 10)
		size := binary.PutUvarint(buf, val)
		require.Equal(b, desiredSize, size, "%d. min: %x, val: %x, expected_size: %d, size: %d", i, min, val, desiredSize, size)

		toDecode[i] = ve{val, buf}
	}

	return toDecode
}

func BenchmarkUnrolledDecodeUVarint(b *testing.B) {
	const DecodesPerTest = 10000000
	toDecode := initToDecode(b, DecodesPerTest)

	type result struct {
		size int
		val  uint64
	}

	decodeBenchmark := []struct {
		name       string
		decodeFunc func([]byte) (uint64, int)
		results    []result
	}{
		{"binary.UVarint", binary.Uvarint, make([]result, len(toDecode))},
		{"unrolled", Uvarint, make([]result, len(toDecode))},
	}

	b.ResetTimer()
	for _, decodeBench := range decodeBenchmark {
		b.Run(decodeBench.name, func(b *testing.B) {
			for i, valAndEnc := range toDecode {
				val, size := decodeBench.decodeFunc(valAndEnc.encoding)
				decodeBench.results[i] = result{size, val}
			}
		})
	}
	b.StopTimer()

	for _, decodeBench := range decodeBenchmark {
		for i, valAndEnc := range toDecode {
			assert.Equal(b, valAndEnc.val, decodeBench.results[i].val)
			assert.Equal(b, i%10+1, decodeBench.results[i].size)
		}
	}
}

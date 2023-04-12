package splunker

import (
	"encoding/binary"
	"io"
	"log"

	"github.com/fionera/splunker/varint"
)

type Header struct {
	Version       byte
	AlignBits     byte
	BaseIndexTime int32
}

func (jd *JournalDecoder) headerDecoder(r *CountedReader, o byte) error {
	var h Header
	if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
		return err
	}

	log.Printf("Journal %s - Version: %d", jd.n, h.Version)
	alignMask := (1 << h.AlignBits) - 1
	_ = alignMask //TODO

	return nil
}

// splunkPrivateDecoder is the decoder for OpcodeSplunkPrivate
func (jd *JournalDecoder) splunkPrivateDecoder(r *CountedReader, o byte) error {
	l, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	if _, err := io.CopyN(io.Discard, r, int64(l)); err != nil {
		return err
	}

	return nil
}

func (jd *JournalDecoder) stringFieldDecoder(r *CountedReader) (string, error) {
	l, err := binary.ReadUvarint(r)
	if err != nil {
		return "", err
	}

	return readString(r, l)
}

// hostDecoder is the decoder for OpcodeNewHost
func (jd *JournalDecoder) hostDecoder(r *CountedReader, o byte) error {
	s, err := jd.stringFieldDecoder(r)
	if err != nil {
		return err
	}

	jd.s.fields[o] = append(jd.s.fields[o], s)

	return nil
}

// sourceDecoder is the decoder for OpcodeNewSource
func (jd *JournalDecoder) sourceDecoder(r *CountedReader, o byte) error {
	s, err := jd.stringFieldDecoder(r)
	if err != nil {
		return err
	}
	jd.s.fields[o] = append(jd.s.fields[o], s)

	return nil
}

// sourceTypeDecoder is the decoder for OpcodeNewSourceType
func (jd *JournalDecoder) sourceTypeDecoder(r *CountedReader, o byte) error {
	s, err := jd.stringFieldDecoder(r)
	if err != nil {
		return err
	}
	jd.s.fields[o] = append(jd.s.fields[o], s)

	return nil
}

// stringDecoder is the decoder for OpcodeNewString
func (jd *JournalDecoder) stringDecoder(r *CountedReader, o byte) error {
	s, err := jd.stringFieldDecoder(r)
	if err != nil {
		return err
	}
	jd.s.fields[o] = append(jd.s.fields[o], s)

	return nil
}

// read the data for the following values
// messageLength, streamID, eStorageLen, streamID, streamOffset, streamSubOffset, indexTime, subSeconds, metadataCount
const eventInfoSize = 8*binary.MaxVarintLen64 + decBufSize + hashSize

// eventDecoder is the decoder for OpcodeOldstyleEventWithHash, OpcodeOldstyleEvent
func (jd *JournalDecoder) eventDecoder(r *CountedReader, o byte) (err error) {
	var peekOffset, n int
	peek, err := jd.cr.Peek(eventInfoSize)
	if err != nil {
		return err
	}

	jd.e.messageLength, n = varint.Uvarint(peek[peekOffset:])
	peekOffset += n
	if err != nil {
		return err
	}
	// add our current position to the message length
	// after decoding metadata of the event, the new position will
	// be subtracted
	jd.e.messageLength += uint64(r.pos) + uint64(peekOffset)

	var eStorageLen uint64
	if jd.e.hasExtendedStorage = o&0x4 != 0; jd.e.hasExtendedStorage {
		jd.e.extendedStorageLen, n = varint.Uvarint(peek[peekOffset:])
		peekOffset += n
		if err != nil {
			return err
		}
	}

	if jd.e.hasHash = o&0x01 == 0; jd.e.hasHash {
		copy(jd.e.hash[:], peek[peekOffset:])
		peekOffset += hashSize
	}

	jd.e.streamID = binary.LittleEndian.Uint64(peek[peekOffset:])
	peekOffset += decBufSize

	jd.e.streamOffset, n = varint.Uvarint(peek[peekOffset:])
	peekOffset += n
	if err != nil {
		return err
	}

	jd.e.streamSubOffset, n = varint.Uvarint(peek[peekOffset:])
	peekOffset += n
	if err != nil {
		return err
	}

	jd.e.indexTime, n = varint.Varint(peek[peekOffset:]) // TODO: +basetime
	peekOffset += n
	if err != nil {
		return err
	}
	// Add the current baseTime
	jd.e.indexTime += int64(jd.s.baseTime)

	jd.e.subSeconds, n = varint.Uvarint(peek[peekOffset:])
	peekOffset += n
	if err != nil {
		return err
	}

	jd.e.metadataCount, n = varint.Uvarint(peek[peekOffset:])
	peekOffset += n
	if err != nil {
		return err
	}

	_, err = jd.cr.Discard(peekOffset)
	if err != nil {
		return err
	}

	// read all into a buffer to prevent tons of ReadByte calls
	// per entry: 1
	// max highest int needed: 3
	// max var int size: MaxVarintLen64
	peek, err = r.Peek(4 * binary.MaxVarintLen64 * int(jd.e.metadataCount))
	if err != nil {
		return err
	}

	peekOffset = 0
	for i := 0; i < int(jd.e.metadataCount); i++ {
		n, err := readMetadata(peek[peekOffset:], o)
		if err != nil {
			return err
		}
		peekOffset += n
	}

	if _, err := r.Discard(peekOffset); err != nil {
		return err
	}

	if jd.e.hasExtendedStorage {
		eStorage, err := readString(r, eStorageLen)
		if err != nil {
			return err
		}
		log.Fatal(eStorage)
	}

	jd.e.messageLength = jd.e.messageLength - uint64(r.pos)
	if cap(jd.e.message) < int(jd.e.messageLength) {
		// create a new byte slice double the size
		// that way we reduce re allocating the slice
		jd.e.message = make([]byte, jd.e.messageLength*2)
	}
	jd.e.message = jd.e.message[:jd.e.messageLength]

	// read the actual message
	_, err = r.Read(jd.e.message)
	if err != nil {
		return err
	}

	jd.e.includePunctuation = (o & 0x22) == 34

	return nil
}

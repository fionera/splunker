package splunker

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

func NewJournalDecoder(name string) (*JournalDecoder, error) {
	r, err := openJournal(name)
	if err != nil {
		return nil, err
	}

	jd := &JournalDecoder{
		n:  name,
		cr: newCountedReader(r),
	}
	jd.s.fields = make(map[byte][]string)

	return jd, nil
}

// to buffer up to uint64 reads
const decBufSize = 8

type JournalDecoder struct {
	cr     *CountedReader
	err    error
	opcode byte
	e      Event
	decBuf [decBufSize]byte
	n      string
	s      struct {
		fields           map[byte][]string
		baseTime         int32
		activeHost       uint64
		activeSource     uint64
		activeSourceType uint64
	}
}

func (jd *JournalDecoder) Host() string {
	return jd.s.fields[byte(OpcodeNewHost)][jd.s.activeHost-1]
}

func (jd *JournalDecoder) Source() string {
	return jd.s.fields[byte(OpcodeNewSource)][jd.s.activeSource-1]
}

func (jd *JournalDecoder) SourceType() string {
	return jd.s.fields[byte(OpcodeNewSourceType)][jd.s.activeSourceType-1]
}

func (jd *JournalDecoder) Scan() bool {
next:
	jd.opcode, jd.err = jd.cr.ReadByte()
	if jd.err != nil {
		return false
	}

	if jd.isEventOpcode() {
		jd.e.reset()
	}

	jd.err = jd.decodeNext()
	if jd.err != nil {
		return false
	}

	if !jd.isEventOpcode() {
		goto next
	}

	return true
}

func (jd *JournalDecoder) Err() error {
	if jd.err == io.EOF {
		return nil
	}
	return jd.err
}

func (jd *JournalDecoder) decodeNewState() (err error) {
	// active host
	if jd.opcode&0x8 != 0 {
		jd.s.activeHost, err = binary.ReadUvarint(jd.cr)
		if err != nil {
			return err
		}
	}

	// active source
	if jd.opcode&0x4 != 0 {
		jd.s.activeSource, err = binary.ReadUvarint(jd.cr)
		if err != nil {
			return err
		}
	}

	// active source type
	if jd.opcode&0x2 != 0 {
		jd.s.activeSourceType, err = binary.ReadUvarint(jd.cr)
		if err != nil {
			return err
		}
	}

	// base time
	if jd.opcode&0x1 != 0 {
		err = binary.Read(jd.cr, binary.LittleEndian, &jd.s.baseTime)
		if err != nil {
			return err
		}
	}

	return nil
}

func (jd *JournalDecoder) decodeNext() error {
	if d := fetchDecoder(Opcode(jd.opcode)); d != nil {
		return d.Decode(jd, jd.cr, jd.opcode)
	}

	if jd.opcode >= 17 && jd.opcode <= 31 {
		return jd.decodeNewState()
	}

	if jd.isEventOpcode() {
		return jd.eventDecoder(jd.cr, jd.opcode)
	}

	return fmt.Errorf("unknown opcode: 0x%02x", jd.opcode)
}

func (jd *JournalDecoder) isEventOpcode() bool {
	return jd.opcode == byte(OpcodeOldstyleEvent) || jd.opcode == byte(OpcodeOldstyleEventWithHash) || (jd.opcode >= 32 && jd.opcode <= 43)
}

const hashSize = 20

type Event struct {
	messageLength      uint64
	hasExtendedStorage bool
	extendedStorageLen uint64
	hasHash            bool
	hash               [hashSize]byte
	streamID           uint64
	streamOffset       uint64
	streamSubOffset    uint64
	indexTime          int64
	subSeconds         uint64
	metadataCount      uint64
	message            []byte
	includePunctuation bool
}

func (e Event) String() string {
	return fmt.Sprintf(
		"messageLength: %v - "+
			"extendedStorageLen: %v - "+
			"hash: %02x - "+
			"streamID: %v - "+
			"streamOffset: %v - "+
			"streamSubOffset: %v - "+
			"indexTime: %v - "+
			"subSeconds: %v - "+
			"metadataCount: %v - "+
			"message: %v - "+
			"includePunctuation: %v",
		e.messageLength,
		e.extendedStorageLen,
		e.hash,
		e.streamID,
		e.streamOffset,
		e.streamSubOffset,
		e.indexTime,
		e.subSeconds,
		e.metadataCount,
		e.MessageString(),
		e.includePunctuation,
	)
}

func (e Event) MessageString() string {
	return bytesToStr(e.message)
}

func (e Event) reset() {
	e.messageLength = 0
	e.hasExtendedStorage = false
	e.extendedStorageLen = 0
	e.hasHash = false
	e.streamID = 0
	e.streamOffset = 0
	e.streamSubOffset = 0
	e.indexTime = 0
	e.subSeconds = 0
	e.metadataCount = 0
	e.message = e.message[:0]
	e.includePunctuation = false
}

// Event returns a struct filled with the current event data.
// calling Scan again will fill it with the next event data
func (jd *JournalDecoder) Event() Event {
	return jd.e
}

func openJournal(name string) (io.Reader, error) {
	p := filepath.Join(name, "rawdata", "journal.zst")
	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}

	zstdReader, err := zstd.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("zstd.NewReader: %v", err)
	}

	return zstdReader, nil
}

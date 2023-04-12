package splunker

//go:generate stringer -type=Opcode
type Opcode byte

const (
	OpcodeNop                   Opcode = 0
	OpcodeOldstyleEvent         Opcode = 1
	OpcodeOldstyleEventWithHash Opcode = 2
	OpcodeNewHost               Opcode = 3
	OpcodeNewSource             Opcode = 4
	OpcodeNewSourceType         Opcode = 5
	OpcodeNewString             Opcode = 6
	OpcodeDelete                Opcode = 8
	OpcodeSplunkPrivate         Opcode = 9
	OpcodeHeader                Opcode = 10
	OpcodeHashSlice             Opcode = 11
)

func fetchDecoder(o Opcode) Decoder {
	switch o {
	case OpcodeHeader:
		return decoderFunc((*JournalDecoder).headerDecoder)
	case OpcodeSplunkPrivate:
		return decoderFunc((*JournalDecoder).splunkPrivateDecoder)
	case OpcodeNewHost:
		return decoderFunc((*JournalDecoder).hostDecoder)
	case OpcodeNewSource:
		return decoderFunc((*JournalDecoder).sourceDecoder)
	case OpcodeNewSourceType:
		return decoderFunc((*JournalDecoder).sourceTypeDecoder)
	case OpcodeNewString:
		return decoderFunc((*JournalDecoder).stringDecoder)
	case OpcodeOldstyleEvent:
		return decoderFunc((*JournalDecoder).eventDecoder)
	case OpcodeOldstyleEventWithHash:
		return decoderFunc((*JournalDecoder).eventDecoder)
	case OpcodeNop:
		return decoderFunc(nil)
	}
	return nil
}

type Decoder interface {
	Decode(*JournalDecoder, *CountedReader, byte) error
}

type decoderFunc func(*JournalDecoder, *CountedReader, byte) error

func (d decoderFunc) Decode(j *JournalDecoder, r *CountedReader, o byte) error {
	if d == nil {
		return nil
	}

	return d(j, r, o)
}

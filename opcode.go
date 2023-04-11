package main

import (
	"fmt"
	"log"
)

//go:generate stringer -type=Opcode
type Opcode int

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

var opcodes = map[Opcode]Decoder{
	OpcodeHeader:                decoderFunc(headerDecoder),
	OpcodeSplunkPrivate:         decoderFunc(splunkPrivateDecoder),
	OpcodeNewHost:               decoderFunc(hostDecoder),
	OpcodeNewSource:             decoderFunc(sourceDecoder),
	OpcodeNewSourceType:         decoderFunc(sourceTypeDecoder),
	OpcodeNewString:             decoderFunc(stringDecoder),
	OpcodeOldstyleEvent:         decoderFunc(eventDecoder),
	OpcodeOldstyleEventWithHash: decoderFunc(eventDecoder),
	OpcodeNop: decoderFunc(func(*CountedReader, Opcode) error {
		return nil
	}),
}

type Decoder interface {
	Decode(*CountedReader, Opcode) error
}

type decoderFunc func(*CountedReader, Opcode) error

func (d decoderFunc) Decode(r *CountedReader, o Opcode) error {
	return d(r, o)
}

func (o Opcode) Decode(r *CountedReader) error {
	if d, ok := opcodes[o]; ok {
		return d.Decode(r, o)
	}

	if o >= 17 && o <= 31 {
		log.Println("4")

		if o&0x8 != 0 {
			log.Println("GET_ACTIVE_HOST")
			asInt, err := readVariableWidthIntAsInt(r)
			if err != nil {
				return err
			}
			log.Printf("Set %d as active host", asInt)
		}

		if o&0x4 != 0 {
			log.Println("GET_ACTIVE_SOURCE")
			asInt, err := readVariableWidthIntAsInt(r)
			if err != nil {
				return err
			}
			log.Printf("Set %d as active source", asInt)
		}
		if o&0x2 != 0 {
			log.Println("GET_ACTIVE_SOURCETYPE")
			asInt, err := readVariableWidthIntAsInt(r)
			if err != nil {
				return err
			}
			log.Printf("Set %d as active source type", asInt)
		}
		if o&0x1 != 0 {
			log.Println("GET_ACTIVE_TIME")
			asInt, err := read32BitLong(r)
			if err != nil {
				return err
			}
			log.Printf("Set %d as active time", asInt)
		}

		return nil
	}

	if o >= 32 && o <= 43 {
		log.Println("5")
		return eventDecoder(r, o)
	}

	return fmt.Errorf("unknown opcode: 0x%02x", int32(o))
}

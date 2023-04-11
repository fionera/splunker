package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

type Header struct {
	Version       byte
	AlignBits     byte
	BaseIndexTime int32
}

func headerDecoder(r *CountedReader, o Opcode) error {
	var h Header
	if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
		return err
	}

	log.Printf("Parsed header of journal file. Version: %d", h.Version)
	alignMask := (1 << h.AlignBits) - 1
	_ = alignMask //TODO

	return nil
}

// splunkPrivateDecoder is the decoder for OpcodeSplunkPrivate
func splunkPrivateDecoder(r *CountedReader, o Opcode) error {
	l, err := readVariableWidthLong(r)
	if err != nil {
		return err
	}

	if _, err := io.CopyN(io.Discard, r, l); err != nil {
		return err
	}

	return nil
}

func stringFieldDecoder(r *CountedReader) (string, error) {
	l, err := readVariableWidthLong(r)
	if err != nil {
		return "", err
	}

	return readString(r, l)
}

// hostDecoder is the decoder for OpcodeNewHost
func hostDecoder(r *CountedReader, o Opcode) error {
	s, err := stringFieldDecoder(r)
	if err != nil {
		return err
	}

	log.Printf("Host: %s", s)
	return nil
}

// sourceDecoder is the decoder for OpcodeNewSource
func sourceDecoder(r *CountedReader, o Opcode) error {
	s, err := stringFieldDecoder(r)
	if err != nil {
		return err
	}

	log.Printf("Source: %s", s)
	return nil
}

// sourceTypeDecoder is the decoder for OpcodeNewSourceType
func sourceTypeDecoder(r *CountedReader, o Opcode) error {
	s, err := stringFieldDecoder(r)
	if err != nil {
		return err
	}

	log.Printf("SourceType: %s", s)
	return nil
}

// stringDecoder is the decoder for OpcodeNewString
func stringDecoder(r *CountedReader, o Opcode) error {
	s, err := stringFieldDecoder(r)
	if err != nil {
		return err
	}

	log.Printf("SourceType: %s", s)
	return nil
}

// eventDecoder is the decoder for OpcodeOldstyleEventWithHash, OpcodeOldstyleEvent
func eventDecoder(r *CountedReader, o Opcode) error {
	endPos, err := readVariableWidthLong(r)
	if err != nil {
		return err
	}
	log.Printf("endOfEvent: %v", endPos)
	endPos += int64(r.pos)

	var eStorageLen int64
	if hasEstorage := o&0x4 != 0; hasEstorage {
		el, err := readVariableWidthIntAsInt(r)
		if err != nil {
			return err
		}
		eStorageLen = int64(el)
		log.Printf("estorage: %v", el)
	}

	if hasHash := o&0x01 == 0; hasHash {
		var buf [20]byte
		if _, err := r.Read(buf[:]); err != nil {
			return err
		}
		log.Printf("hash: %02x", buf)
	}

	var streamId uint64
	if err := binary.Read(r, binary.LittleEndian, &streamId); err != nil {
		return err
	}
	log.Printf("streamId: %v", streamId)

	streamOffset, err := readVariableWidthLong(r)
	if err != nil {
		return err
	}
	log.Printf("streamOffset: %v", streamOffset)

	streamSubOffset, err := readVariableWidthInt(r)
	if err != nil {
		return err
	}
	log.Printf("streamSubOffset: %v", streamSubOffset)

	indexTime, err := readVariableWidthSignedInt(r) // TODO: +basetime
	if err != nil {
		return err
	}
	log.Printf("indexTime: %v", indexTime)

	subSeconds, err := readVariableWidthLong(r)
	if err != nil {
		return err
	}
	log.Printf("subSeconds: %v", subSeconds)

	metadataCount, err := readVariableWidthInt(r)
	if err != nil {
		return err
	}

	log.Printf("metadataCount: %v", metadataCount)
	for i := 0; i < int(metadataCount); i++ {
		err := readMetadata(r, o)
		if err != nil {
			return err
		}
	}

	if eStorageLen > 0 {
		eStorage, err := readString(r, eStorageLen)
		if err != nil {
			return err
		}
		log.Println(eStorage)
	}

	re, err := readString(r, endPos-int64(r.pos))
	if err != nil {
		return err
	}
	log.Println("rawEvent", re)
	fmt.Println(re)

	log.Println("includePunctuation", (o&0x22) == 34)

	return nil
}

package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"io"
	"log"
	"os"
	"path/filepath"
)

const baseDir = "./data"

func main() {
	if err := readDB(filepath.Join(baseDir, "db")); err != nil {
		log.Fatal(err)
	}

	if err := readDB(filepath.Join(baseDir, "colddb")); err != nil {
		log.Fatal(err)
	}
}

func readDB(name string) error {
	dir, err := os.ReadDir(name)
	if err != nil {
		return err
	}

	for _, e := range dir {
		if !e.IsDir() {
			continue
		}

		if err := readChunk(filepath.Join(name, e.Name())); err != nil {
			return err
		}
	}

	return nil
}

func readChunk(name string) error {
	f, err := os.Open(filepath.Join(name, "rawdata", "journal.zst"))
	if err != nil {
		return err
	}

	reader, err := zstd.NewReader(f)
	if err != nil {
		return err
	}

	if err := readHeader(reader); err != nil {
		return err
	}

	for i := 0; i < 0x1E; i++ {
		if err := readField(reader); err != nil {
			return err
		}
	}

	skipBytes(reader, 0x95)

	n, err := io.CopyN(hex.NewEncoder(os.Stdout), reader, 1024)
	if err != nil {
		return err
	}
	_ = n

	return fmt.Errorf("return")
}

func readHeader(r io.Reader) error {
	skipBytes(r, 1)

	var t int8 // whats this? The type?
	if err := binary.Read(r, binary.LittleEndian, &t); err != nil {
		return err
	}

	var l int8 // only int8?
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return err
	}

	skipBytes(r, 0x12)

	s, err := readString(r, int(l))
	if err != nil {
		return err
	}

	log.Printf("%0x %d %q", t, l, s)
	return nil
}

func skipBytes(r io.Reader, l int) error {
	_, err := io.CopyN(io.Discard, r, int64(l))
	if err != nil {
		return err
	}
	return nil
}

func readField(r io.Reader) error {
	var t int8 // whats this? The type?
	if err := binary.Read(r, binary.LittleEndian, &t); err != nil {
		return err
	}

	var l int8 // only int8?
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return err
	}

	s, err := readString(r, int(l))
	if err != nil {
		return err
	}

	log.Printf("%0x %d %q", t, l, s)
	return nil
}

func readString(r io.Reader, length int) (string, error) {
	b := make([]byte, length)
	_, err := r.Read(b)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

package main

import (
	"bufio"
	"encoding/binary"
	"github.com/klauspost/compress/zstd"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
)

const baseDir = "./adsb_bratwurst"

func main() {
	go http.ListenAndServe(":8080", nil)

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(io.Discard)
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
			log.Println(err)
		}
	}

	return nil
}

func readChunk(name string) error {
	file, err := os.Open(filepath.Join(name, "rawdata", "journal.zst"))
	if err != nil {
		return err
	}

	zstdReader, err := zstd.NewReader(file)
	if err != nil {
		return err
	}

	countReader := newCountedReader(zstdReader)

	for {
		var nextOpCode byte
		if err := binary.Read(countReader, binary.LittleEndian, &nextOpCode); err != nil {
			return err
		}

		log.Printf("next: 0x%02x - %s", nextOpCode, Opcode(nextOpCode))
		if err := Opcode(nextOpCode).Decode(countReader); err != nil {
			return err
		}
	}

	return nil
}

type CountedReader struct {
	pos int
	r   *bufio.Reader
}

func (c *CountedReader) ReadByte() (b byte, err error) {
	b, err = c.r.ReadByte()
	c.pos++
	return
}

func (c *CountedReader) Read(p []byte) (n int, err error) {
	n, err = io.ReadFull(c.r, p)
	c.pos += n
	return
}

func newCountedReader(r io.Reader) *CountedReader {
	return &CountedReader{r: bufio.NewReaderSize(r, 8*4096)}
}

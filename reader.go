package splunker

import (
	"bufio"
	"io"
)

type CountedReader struct {
	pos int
	r   *bufio.Reader
}

func (c *CountedReader) Peek(n int) ([]byte, error) {
	return c.r.Peek(n)
}

func (c *CountedReader) Discard(n int) (int, error) {
	c.pos += n
	return c.r.Discard(n)
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

package main

import (
	"io"
	//"log"
)

type RangeReader struct {
	readerAt io.ReaderAt
	offset   int64
	endOffset int64
}

func (this *RangeReader) Read(p []byte) (n int, err error) {
	if this.endOffset >= 0 {
		remainingByteCount := this.endOffset - this.offset

		if remainingByteCount == 0 {
			return 0, io.EOF
		} else if remainingByteCount < int64(len(p)) {
			p = p[0:remainingByteCount]
		}
	}

	n, err = this.readerAt.ReadAt(p, this.offset)
	this.offset += int64(n)

	return
}

/*
func (this *RangeReader) ReadAt(p []byte, offset int64) (n int, err error) {
	n, err = this.readerAt.ReadAt(p, offset)

	if this.endOffset >= 0 {
		remainingByteCount := this.endOffset - this.offset

		if int64(n) > remainingByteCount {
			if err == nil { 
				return int(remainingByteCount), io.ErrUnexpectedEOF
			} else {
				return int(remainingByteCount), err
			}
		}
	}

	return
}
*/

func NewRangeReader(readerAt io.ReaderAt, offset int64, endOffset int64) io.Reader {
	return &RangeReader{
		readerAt: readerAt,
		offset:   offset,
		endOffset: endOffset,
	}
}
/*
type ReaderReaderAt interface {
	io.Reader
	io.ReaderAt
}
*/
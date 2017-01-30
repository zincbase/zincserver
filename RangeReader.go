package main

import (
	"io"
)

type RangeReader struct {
	readerAt   io.ReaderAt
	readOffset int64
	endOffset  int64
}

func (this *RangeReader) Read(p []byte) (n int, err error) {
	remainingByteCount := this.endOffset - this.readOffset

	if remainingByteCount <= 0 {
		return 0, io.EOF
	} else if remainingByteCount < int64(len(p)) {
		p = p[0:remainingByteCount]
	}

	n, err = this.readerAt.ReadAt(p, this.readOffset)
	this.readOffset += int64(n)

	return
}

func NewRangeReader(readerAt io.ReaderAt, startOffset int64, endOffset int64) io.Reader {
	return &RangeReader{
		readerAt:   readerAt,
		readOffset: startOffset,
		endOffset:  endOffset,
	}
}

package main

import (
	"io"
	//"log"
)

const MaxPrefetchSize = 32768

type PrefetchingReaderAt struct {
	readerAt io.ReaderAt

	prefetchStartOffset int64
	prefetchEndOffset   int64
	prefetchBuffer      []byte
}

func (this *PrefetchingReaderAt) ReadAt(p []byte, readOffset int64) (int, error) {
	readLength := len(p)

	if readOffset >= this.prefetchStartOffset && readOffset+int64(readLength) <= this.prefetchEndOffset {
		copy(p, this.prefetchBuffer[readOffset-this.prefetchStartOffset:])
		return readLength, nil
	} else if readLength < MaxPrefetchSize/2 {
		bytesPrefetched, err := this.readerAt.ReadAt(this.prefetchBuffer, readOffset)

		if err != nil && (err != io.EOF || bytesPrefetched == 0) {
			return 0, err
		}

		this.prefetchStartOffset = readOffset
		this.prefetchEndOffset = readOffset + int64(bytesPrefetched)

		bytesRead := MinInt(readLength, bytesPrefetched)
		copy(p, this.prefetchBuffer[0:bytesRead])
		return bytesRead, nil
	} else {
		return this.readerAt.ReadAt(p, readOffset)
	}
}

func NewPrefetchingReaderAt(readerAt io.ReaderAt) io.ReaderAt {
	return &PrefetchingReaderAt{
		readerAt: readerAt,

		prefetchStartOffset: -1,
		prefetchEndOffset:   -1,
		prefetchBuffer:      make([]byte, MaxPrefetchSize),
	}
}

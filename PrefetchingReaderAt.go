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
	// Note that there is an implicit assumption here that subsequent calls to this
	// function would have read offsets that are greater than the previous ones.
	//
	// If that isn't the case then the prefetcher would effectively preform like
	// standard 'ReadAt()'.

	readLength := len(p)

	// If the requested range is within the prefetch buffer, copy a slice from the buffer
	if readOffset >= this.prefetchStartOffset && readOffset+int64(readLength) <= this.prefetchEndOffset {
		copy(p, this.prefetchBuffer[readOffset-this.prefetchStartOffset:])
		return readLength, nil

	// Otherwise if the requested size smaller than half the maximum prefetch buffer size,
	// prefetch as much as possible and store the range, then return a copy of the data that was read
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

		
	} else { // Otherwise just return the result of ReadAt
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

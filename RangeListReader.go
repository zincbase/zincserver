package main

import (
	"io"
	//"log"
)

type Range struct {
	startOffset int64
	endOffset int64
}

type RangeListReader struct {
	readerAt io.ReaderAt
	rangeList []Range
}

func (this *RangeListReader) Read(p []byte) (int, error) {
	writeOffset := 0

	for writeOffset < len(p) { 
		if len(this.rangeList) == 0 {
			return writeOffset, io.EOF
		}

		currentRange := &this.rangeList[0]
		readLength := MinInt(int(currentRange.endOffset - currentRange.startOffset), len(p) - writeOffset)
		
		_, err := this.readerAt.ReadAt(p[writeOffset:writeOffset+readLength], currentRange.startOffset)
		if err != nil { return 0, err }

		writeOffset += readLength

		currentRange.startOffset += int64(readLength)
		if currentRange.startOffset == currentRange.endOffset {
			this.rangeList = this.rangeList[1:]
		}
	}

	return writeOffset, nil
}

func NewRangeListReader(readerAt io.ReaderAt, rangeList []Range) io.Reader {
	return &RangeListReader{
		readerAt: readerAt,
		rangeList: rangeList,
	}
}

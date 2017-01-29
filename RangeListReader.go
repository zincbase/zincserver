package main

import (
	"io"
)

type Range struct {
	StartOffset int64
	EndOffset   int64
}

type RangeList []Range

func (this RangeList) Less(a, b int) bool {
	return this[a].StartOffset < this[b].StartOffset
}

func (this RangeList) Swap(a, b int) {
	this[a], this[b] = this[b], this[a]
}

func (this RangeList) Len() int {
	return len(this)
}

type RangeListReader struct {
	readerAt  io.ReaderAt
	rangeList RangeList
}

func (this *RangeListReader) Read(p []byte) (int, error) {
	writeOffset := 0

	for writeOffset < len(p) {
		if len(this.rangeList) == 0 {
			return writeOffset, io.EOF
		}

		currentRange := &this.rangeList[0]
		readLength := MinInt(int(currentRange.EndOffset-currentRange.StartOffset), len(p)-writeOffset)

		_, err := this.readerAt.ReadAt(p[writeOffset:writeOffset+readLength], currentRange.StartOffset)
		if err != nil {
			return 0, err
		}

		writeOffset += readLength

		currentRange.StartOffset += int64(readLength)
		if currentRange.StartOffset == currentRange.EndOffset {
			this.rangeList = this.rangeList[1:]
		}
	}

	return writeOffset, nil
}

func NewRangeListReader(readerAt io.ReaderAt, rangeList []Range) io.Reader {
	return &RangeListReader{
		readerAt:  readerAt,
		rangeList: rangeList,
	}
}

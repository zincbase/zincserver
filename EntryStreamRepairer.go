package main

import (
	"io"
)

func FindSafeTruncationSize(source io.ReaderAt, endOffset int64) (truncatedSize int64, err error) {
	next := NewEntryStreamIterator(source, 0, endOffset, true)

	var previousIteratorResult *EntryStreamIteratorResult

	for {
		iteratorResult, err := next()

		if err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			} else {
				return 0, err
			}
		}

		if previousIteratorResult != nil && previousIteratorResult.HasTransactionEndFlag() {
			truncatedSize = previousIteratorResult.Offset + previousIteratorResult.Size
		}

		if iteratorResult == nil {
			break
		}

		previousIteratorResult = iteratorResult
	}

	return
}

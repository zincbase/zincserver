package main

import (
	"io"
)

func FindSafeTruncationSize(source io.ReaderAt, endOffset int64) (truncatedSize int64, err error) {
	next := NewEntryStreamIterator(source, 0, endOffset)

	for {
		iteratorResult, err := next()

		if err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			} else {
				return 0, err
			}
		}

		if iteratorResult == nil {
			break
		}

		if !iteratorResult.VerifyPrimaryHeaderChecksum() || !iteratorResult.VerifyPayloadChecksum() {
			break
		}

		if iteratorResult.HasTransactionEndFlag() {
			truncatedSize = iteratorResult.EndOffset()
		}
	}

	return
}

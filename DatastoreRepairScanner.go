package main

import (
	"io"
)

func FindSafeTruncationSize(source io.ReaderAt, endOffset int64) (int64, error) {
	// Create a new iterator
	next := NewEntryStreamIterator(source, 0, endOffset)

	// Iterate once to the first entry
	iteratorResult, err := next()

	// If an error occurred iterating the first entry or it is empty
	if err != nil || iteratorResult == nil {
		// Return a truncation size of 0
		return 0, nil
	}

	// Verify it is a valid head entry
	if !iteratorResult.VerifyValidHeadEntry() {
		// Return a truncation size of 0
		return 0, nil
	}

	// Verify the head entry's checksums
	if !iteratorResult.VerifyAllChecksums() {
		// Return a truncation size of 0
		return 0, nil
	}

	// Read the head entry's value
	headEntryValueBytes, err := iteratorResult.ReadValue()

	// If an error occurred while reading the value
	if err != nil {
		// Return a truncation size of 0
		return 0, nil
	}

	// Deserialize the head entry's value
	headEntryValue := DeserializeHeadEntryValue(headEntryValueBytes)

	// The initial truncation size would now be set to the end of the head entry
	truncationSize := int64(HeadEntrySize)

	// Iterate over the rest of the entries in the datastore
	for {
		// Iterate to the next entry
		iteratorResult, err = next()

		// If an error occurred while iterating
		if err != nil {
			// If the error was an unexpected end of stream
			if err == io.ErrUnexpectedEOF {
				// Return the current truncation size
				return truncationSize, nil
			} else {
				// Otherwise, return the error with a zero truncation size
				return 0, err
			}
		}

		// If the iterator result is empty
		if iteratorResult == nil {
			// Return the current truncation size
			return truncationSize, nil
		}

		// If the checksum of the entry fails
		if !iteratorResult.VerifyAllChecksums() {
			// Return the current truncation size
			return truncationSize, nil
		}

		// If the current entry either has a transaction end flag,
		// or happened before or at the time datastore was last compacted
		if iteratorResult.HasTransactionEndFlag() ||
			iteratorResult.CommitTime() <= headEntryValue.LastCompactionTime {
			// set the truncation size to its end offset
			truncationSize = iteratorResult.EndOffset()
		}
	}
}

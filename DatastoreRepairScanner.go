package main

import (
	"io"
)

// Given an input datastore stream, find the longest truncation length such that the datastore would
// be a valid one
func FindSafeTruncationSize(source io.ReaderAt, endOffset int64) (int64, error) {
	// Create a new iterator
	next := NewEntryStreamIterator(source, 0, endOffset)

	// Iterate once to the first entry
	iteratorResult, err := next()

	// If the stream ended unecpectedly
	if err == io.ErrUnexpectedEOF {
		// Return a truncation size of 0 with no error
		return 0, nil
	} else if err != nil { // Othewise, if some other error occurred
		// Return the error with a truncation size of the end offset
		return endOffset, err
	}

	// If the first entry is empty
	if iteratorResult == nil {
		// Return a truncation size of 0
		return 0, nil
	}

	// Verify the head entry's checksums
	err = iteratorResult.VerifyAllChecksums()

	// If the checksum verification failed
	if err == ErrCorruptedEntry {
		// Return a truncation size of 0
		return 0, nil
	} else if err != nil { // If some other error occurred
		// Return the error and the end offset as truncation size
		return endOffset, err
	}

	// If the first entry isn't a valid head entry
	// (Note that since the checksum verification passed this failing would be a very strange situation)
	err = iteratorResult.VerifyValidHeadEntry()

	if err == ErrInvalidHeadEntry {
		// Return a truncation size of 0
		return 0, nil
	} else if err != nil { // If some other error occurred
		// Return the error and the end offset as truncation size
		return endOffset, err
	}

	// Read the head entry's value
	headEntryValueBytes, err := iteratorResult.ReadValue()

	// If reading the value failed
	if err != nil {
		// Return the error and the end offset as truncation size
		return endOffset, err
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
			} else { // If some other error occurred
				// Return the error with the original end offset as truncation size
				// (there is no way to know what caused the error, it could be a disk or OS error)
				return endOffset, err
			}
		}

		// If the iterator result is empty
		if iteratorResult == nil {
			// Return the current truncation size
			return truncationSize, nil
		}

		// Verify the checksums for the entry
		err = iteratorResult.VerifyAllChecksums()

		// If the checksums failed
		if err == ErrCorruptedEntry {
			// Return the current truncation size
			return truncationSize, nil
		} else if err != nil { // If some other error occurred
			// Return the error with the original end offset as truncation size
			// (there is no way to know what caused the error, it could be a disk or OS error)
			return endOffset, err
		}

		// If the current entry either has a transaction end flag,
		// or happened before or at the time datastore was last compacted
		if iteratorResult.HasTransactionEndFlag() ||
			iteratorResult.CommitTime() <= headEntryValue.LastCompactionTime {
			// Set the truncation size to its end offset
			truncationSize = iteratorResult.EndOffset()
		}
	}
}

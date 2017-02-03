package main

import (
	"bytes"
	"io"
)

// Datastore index entry object type
type DatastoreIndexEntry struct {
	offset    int64
	timestamp int64
}

// Entry validator function type
type DatastoreIndexEntryValidator func(iteratorResult *EntryStreamIteratorResult) error

// Datastore index object type
type DatastoreIndex struct {
	TotalSize int64
	Entries   []DatastoreIndexEntry
	Validator DatastoreIndexEntryValidator
}

// Append the index from a stream of serialized entries
func (this *DatastoreIndex) AddFromEntryStream(source io.ReaderAt, startOffset int64, endOffset int64) error {
	// Create an entry stream iterator
	next := NewEntryStreamIterator(source, startOffset, endOffset)

	// Store the previous commit timestamp
	previousCommitTimestamp := int64(0)

	for {
		// Iterate to the next entry in the stream
		iteratorResult, err := next()

		// If an error occurred when iterating
		if err != nil {
			// Return the error
			return err
		}

		// If returned the iterator object is nil, it means the stream has ended
		if iteratorResult == nil {
			// Return without error
			return nil
		}

		// If the current entry is the last one but it doesn't have a transaction end flag
		if iteratorResult.EndOffset() == endOffset && !iteratorResult.HasTransactionEndFlag() {
			// Return an unexpected end of stream error
			return io.ErrUnexpectedEOF
		}

		// If a custom validator function is defined
		if this.Validator != nil {
			// Apply the validator to the iterator result
			err = this.Validator(iteratorResult)

			// If the validator failed
			if err != nil {
				// Return the error
				return err
			}
		}

		// If the current entry has a commit time that's strictly greater than previous one
		if iteratorResult.PrimaryHeader.CommitTime > previousCommitTimestamp {
			// Add the offset and commit time of that entry to the index
			this.Entries = append(
				this.Entries,
				DatastoreIndexEntry{this.TotalSize, iteratorResult.PrimaryHeader.CommitTime})
		}

		// Update the previous commit time variable
		previousCommitTimestamp = iteratorResult.PrimaryHeader.CommitTime

		// Add the size of the new entry to the total size of the indexed entries
		this.TotalSize += iteratorResult.Size
	}
}

// Append the index from a buffer containing an empty stream
func (this *DatastoreIndex) AppendFromBuffer(entryStreamBuffer []byte) error {
	// Create a reader for the buffer and add from it
	return this.AddFromEntryStream(bytes.NewReader(entryStreamBuffer), 0, int64(len(entryStreamBuffer)))
}

// Find the offset for the first entry that was updated after the given time
func (this *DatastoreIndex) FindOffsetOfFirstEntryUpdatedAfter(lowerBoundingTimestamp int64) int64 {
	// For each entry in the index
	for _, entry := range this.Entries {
		// If the timestamp of this entry is strictly greater than the lower bounding timestamp
		if entry.timestamp > lowerBoundingTimestamp {
			// Return its offset
			return entry.offset
		}
	}

	// Otherwise, return -1
	return -1
}

// Get the most recent update timestamp for the whole datastore
func (this *DatastoreIndex) LatestTimestamp() int64 {
	if len(this.Entries) == 0 {
		return -1
	}

	return this.Entries[len(this.Entries)-1].timestamp
}

// Construct a datastore index with no validator
func NewDatastoreIndex() *DatastoreIndex {
	return &DatastoreIndex{
		TotalSize: 0,
		Entries:   []DatastoreIndexEntry{},
		Validator: nil,
	}
}

// Construct a datastore index with a custom validator
func NewDatastoreIndexWithValidator(validator DatastoreIndexEntryValidator) *DatastoreIndex {
	return &DatastoreIndex{
		TotalSize: 0,
		Entries:   []DatastoreIndexEntry{},
		Validator: validator,
	}
}

// Construct a datastore index with full checksum verification (i.e. both header and payload)
func NewDatastoreIndexWithFullChecksumVerification() *DatastoreIndex {
	return NewDatastoreIndexWithValidator(func(result *EntryStreamIteratorResult) error {
		return result.VerifyAllChecksums()
	})
}

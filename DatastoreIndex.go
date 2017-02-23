package main

import (
	"bytes"
	"io"
)

// Datastore index entry object type
type DatastoreIndexEntry struct {
	timestamp int64
	offset    int64
}

// Entry validator function type
type IteratorResultValidator func(iteratorResult *EntryStreamIteratorResult) error

// Datastore index object type
type DatastoreIndex struct {
	TotalSize int64
	Entries   []DatastoreIndexEntry
}

// Construct a datastore index with no validator
func NewDatastoreIndex() *DatastoreIndex {
	return &DatastoreIndex{
		TotalSize: 0,
		Entries:   []DatastoreIndexEntry{},
	}
}

// Append the index from a stream of serialized entries
func (this *DatastoreIndex) AppendFromEntryStream(source io.ReaderAt, startOffset int64, endOffset int64, validator IteratorResultValidator) error {
	// Create an entry stream iterator
	next := NewEntryStreamIterator(source, startOffset, endOffset)

	// Store the previous commit timestamp
	previousCommitTimestamp := this.LatestTimestamp()

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

		// If a custom validator function has been supplied
		if validator != nil {
			// Apply the validator to the iterator result
			err = validator(iteratorResult)

			// If the validator failed
			if err != nil {
				// Return the error
				return err
			}
		}

		// If the current entry has a commit time that's strictly greater than previous one
		if iteratorResult.CommitTime() > previousCommitTimestamp {
			// Add the offset and commit time of that entry to the index
			this.Entries = append(
				this.Entries,
				DatastoreIndexEntry{
					timestamp: iteratorResult.PrimaryHeader.CommitTime,
					offset: this.TotalSize,
				})

			// Update the previous commit time variable
			previousCommitTimestamp = iteratorResult.CommitTime()
		}

		// Add the size of the new entry to the total size of the indexed entries
		this.TotalSize += iteratorResult.Size
	}
}

// Append the index from a buffer containing an empty stream
func (this *DatastoreIndex) AppendFromBuffer(entryStreamBuffer []byte, validator IteratorResultValidator) error {
	// Create a reader for the buffer and add from it
	return this.AppendFromEntryStream(bytes.NewReader(entryStreamBuffer), 0, int64(len(entryStreamBuffer)), validator)
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

// Clone this index
func (this *DatastoreIndex) Clone() *DatastoreIndex {
	return &DatastoreIndex{
		TotalSize: this.TotalSize,
		Entries:   this.Entries,
	}
}

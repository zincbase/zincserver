package main

import (
	"bytes"
	"io"
	//"log"
)

type DatastoreIndexEntry struct {
	offset    int64
	timestamp int64
}

type DatastoreIndex struct {
	totalSize int64
	entries   []DatastoreIndexEntry
}

func (this *DatastoreIndex) AppendFromReader(source io.ReaderAt, startOffset int64, endOffset int64) error {
	next := NewEntryStreamIterator(source, startOffset, endOffset, true)

	previousTimestamp := int64(0)

	for {
		iteratorResult, err := next()

		if err != nil {
			return err
		}

		if iteratorResult == nil {
			return nil
		}

		if iteratorResult.PrimaryHeader.CommitTime > previousTimestamp {
			this.entries = append(
				this.entries,
				DatastoreIndexEntry{this.totalSize, iteratorResult.PrimaryHeader.CommitTime})
		}

		previousTimestamp = iteratorResult.PrimaryHeader.CommitTime
		this.totalSize += iteratorResult.Size
	}
}

func (this *DatastoreIndex) AppendFromBuffer(entryStreamBuffer []byte) error {
	return this.AppendFromReader(bytes.NewReader(entryStreamBuffer), 0, int64(len(entryStreamBuffer)))
}

func (this *DatastoreIndex) FindOffsetOfFirstEntryUpdatedAfter(lowerBoundingTimestamp int64) int64 {
	for _, entry := range this.entries {
		if entry.timestamp > lowerBoundingTimestamp {
			return entry.offset
		}
	}

	return -1
}

func (this *DatastoreIndex) LatestUpdateTimestamp() int64 {
	if len(this.entries) == 0 {
		return -1
	}

	return this.entries[len(this.entries)-1].timestamp
}

func (this *DatastoreIndex) LatestEntryOffset() int64 {
	if len(this.entries) == 0 {
		return 0
	}

	return this.entries[len(this.entries)-1].offset
}

func NewDatastoreIndex() *DatastoreIndex {
	return &DatastoreIndex{
		totalSize: 0,
		entries:   []DatastoreIndexEntry{},
	}
}

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
	TotalSize int64
	Entries   []DatastoreIndexEntry
}

func (this *DatastoreIndex) AddFromEntryStream(source io.ReaderAt, startOffset int64, endOffset int64) error {
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
			this.Entries = append(
				this.Entries,
				DatastoreIndexEntry{this.TotalSize, iteratorResult.PrimaryHeader.CommitTime})
		}

		previousTimestamp = iteratorResult.PrimaryHeader.CommitTime
		this.TotalSize += iteratorResult.Size
	}
}

func (this *DatastoreIndex) AppendFromBuffer(entryStreamBuffer []byte) error {
	return this.AddFromEntryStream(bytes.NewReader(entryStreamBuffer), 0, int64(len(entryStreamBuffer)))
}

func (this *DatastoreIndex) FindOffsetOfFirstEntryUpdatedAfter(lowerBoundingTimestamp int64) int64 {
	for _, entry := range this.Entries {
		if entry.timestamp > lowerBoundingTimestamp {
			return entry.offset
		}
	}

	return -1
}

func (this *DatastoreIndex) LatestUpdateTimestamp() int64 {
	if len(this.Entries) == 0 {
		return -1
	}

	return this.Entries[len(this.Entries)-1].timestamp
}

func NewDatastoreIndex() *DatastoreIndex {
	return &DatastoreIndex{
		TotalSize: 0,
		Entries:   []DatastoreIndexEntry{},
	}
}

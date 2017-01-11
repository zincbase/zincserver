package main

import (
	"crypto/sha1"
	"io"
	"sort"
)

type DatastoreKeyIndex struct {
	keyIndex map[string]Range
}

func NewDatastoreKeyIndex() *DatastoreKeyIndex {
	return &DatastoreKeyIndex{
		keyIndex: make(map[string]Range),
	}
}

func (this *DatastoreKeyIndex) AddFromEntryStream(entryStream io.ReaderAt, startOffset int64, endOffset int64) error {
	next := NewEntryStreamIterator(entryStream, startOffset, endOffset, false)

	for {
		iteratorResult, err := next()

		if err != nil {
			return err
		}

		if iteratorResult == nil {
			return nil
		}

		key, err := iteratorResult.ReadKey()
		if err != nil {
			return err
		}

		this.Set(key, iteratorResult.Offset, iteratorResult.Offset+iteratorResult.Size)
	}
}

func (this *DatastoreKeyIndex) Set(key []byte, startOffset int64, endOffset int64) {
	this.keyIndex[SHA1(key)] = Range{startOffset, endOffset}
}

func (this *DatastoreKeyIndex) Get(key []byte) (result Range, exists bool) {
	result, exists = this.keyIndex[SHA1(key)]
	return
}

func SHA1(data []byte) string {
	hash := sha1.New()
	return string(hash.Sum(data))
}

func (this *DatastoreKeyIndex) GetCompactedRanges(readOffset int64, consolidate bool) RangeArray {
	compactedRanges := RangeArray{}

	for _, currentRange := range this.keyIndex {
		if currentRange.endOffset <= readOffset {
			continue
		} else if currentRange.startOffset < readOffset {
			currentRange.startOffset = readOffset
		}

		compactedRanges = append(compactedRanges, currentRange)
	}

	sort.Sort(compactedRanges)

	if consolidate {
		consolidatedRanges := RangeArray{}
		for _, currentRange := range compactedRanges {
			currentLength := len(consolidatedRanges)

			if currentLength > 0 && currentRange.startOffset == consolidatedRanges[currentLength-1].endOffset {
				consolidatedRanges[currentLength-1].endOffset = currentRange.endOffset
			} else {
				consolidatedRanges = append(consolidatedRanges, currentRange)
			}
		}
		
		compactedRanges = consolidatedRanges
	}

	return compactedRanges
}

func (this *DatastoreKeyIndex) GetCompactedSize() (compactedSize int64) {
	compactedSize = 0

	for _, entry := range this.keyIndex {
		compactedSize += entry.endOffset - entry.startOffset
	}

	return
}

func (this *DatastoreKeyIndex) CreateReaderForCompactedRanges(entryStream io.ReaderAt, startOffset int64) io.Reader {
	return NewRangeListReader(entryStream, this.GetCompactedRanges(startOffset, true))
}

func (this *DatastoreKeyIndex) CompactToBuffer(entryStream io.ReaderAt, startOffset int64) (result []byte, err error) {
	memoryWriter := NewMemoryWriter()

	_, err = io.Copy(memoryWriter, this.CreateReaderForCompactedRanges(entryStream, startOffset))
	if err != nil {
		return
	}

	result = memoryWriter.WrittenData()
	return
}

type RangeArray []Range

func (this RangeArray) Less(a, b int) bool {
	return this[a].startOffset < this[b].startOffset
}

func (this RangeArray) Swap(a, b int) {
	this[a], this[b] = this[b], this[a]
}

func (this RangeArray) Len() int {
	return len(this)
}

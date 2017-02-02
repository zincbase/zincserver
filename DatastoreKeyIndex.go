package main

import (
	"io"
	"sort"
)

// An object containing a map that translates SHA1 key hashes to ranges
type DatastoreKeyIndex struct {
	keyIndex map[string]Range
}

// A constructor for the key index object
func NewDatastoreKeyIndex() *DatastoreKeyIndex {
	return &DatastoreKeyIndex{
		keyIndex: make(map[string]Range),
	}
}

// Add to the key index from the given entry stream
func (this *DatastoreKeyIndex) AddFromEntryStream(entryStream io.ReaderAt, startOffset int64, endOffset int64) error {
	// Create an iterator
	next := NewEntryStreamIterator(entryStream, startOffset, endOffset)

	// Loop
	for {
		// Get the next result
		iteratorResult, err := next()

		// If an error occurred while getting the next result
		if err != nil {
			// Return the error
			return err
		}

		// If the iterator finished
		if iteratorResult == nil {
			// Return without error
			return nil
		}

		// Read the key of the current entry
		key, err := iteratorResult.ReadKey()

		// If an error occurred while reading the key
		if err != nil {
			// Return the error
			return err
		}

		// Add a map entry for this key associated with its start and end offset
		this.Set(key, iteratorResult.Offset, iteratorResult.EndOffset())
	}
}

// Set a key in the map to the given range
func (this *DatastoreKeyIndex) Set(key []byte, startOffset int64, endOffset int64) {
	this.keyIndex[SHA1(key)] = Range{startOffset, endOffset}
}

// Get the range associated with the given key, if exists
func (this *DatastoreKeyIndex) Get(key []byte) (result Range, exists bool) {
	result, exists = this.keyIndex[SHA1(key)]
	return
}

// Scan all the entries in this map to get a list of compacted ranges. Optionally consolidate
// subsequent ranges that are directly adjacent to each other, e.g. [5:10] and [10:17]
func (this *DatastoreKeyIndex) GetCompactedRanges(readOffset int64, consolidate bool) RangeList {
	// Create an new empty list of ranges
	compactedRanges := RangeList{}

	// For each key in the map
	for _, currentRange := range this.keyIndex {
		// If the range is outside the requested range
		if currentRange.EndOffset <= readOffset || currentRange.StartOffset < readOffset {
			// Continue to the next range
			continue
		}

		// Add the range to the list
		compactedRanges = append(compactedRanges, currentRange)
	}

	// Sort the ranges
	sort.Sort(compactedRanges)

	// If consolidate option was enabled
	if consolidate {
		// Create a list of consolidate ranges
		consolidatedRanges := RangeList{}

		// For each range in the compacted ranges
		for _, currentRange := range compactedRanges {
			// Get the current length of the consolidated ranges list
			length := len(consolidatedRanges)

			// If the end offset of the previous range equals the start offset of the current one
			if length > 0 && currentRange.StartOffset == consolidatedRanges[length-1].EndOffset {
				// Extend the last range to include the current one
				consolidatedRanges[length-1].EndOffset = currentRange.EndOffset
			} else {
				// Otherwise add the current range to the list
				consolidatedRanges = append(consolidatedRanges, currentRange)
			}
		}

		// Use the consolidated ranges as the compacted ranges to return
		compactedRanges = consolidatedRanges
	}

	return compactedRanges
}

// Get the total size of all the ranges indexed
func (this *DatastoreKeyIndex) GetCompactedSize() (compactedSize int64) {
	compactedSize = 0

	// For each entry
	for _, entry := range this.keyIndex {
		// Add its size to the total
		compactedSize += entry.EndOffset - entry.StartOffset
	}

	return
}

// Create a reader for the result of the compaction of the given stream, starting at the given offset
func (this *DatastoreKeyIndex) CreateReaderForCompactedRanges(entryStream io.ReaderAt, startOffset int64) io.Reader {
	// Create a new range list reader to the result of compacting and consolidating
	// all the calculated ranges for the given stream
	return NewRangeListReader(entryStream, this.GetCompactedRanges(startOffset, true))
}

// Compact the given entry stream to a buffer
func (this *DatastoreKeyIndex) CompactToBuffer(entryStream io.ReaderAt, startOffset int64) (result []byte, err error) {
	// Create a new memory writer
	memoryWriter := NewMemoryWriter()

	// Copy the result of the operation in the writer
	_, err = io.Copy(memoryWriter, this.CreateReaderForCompactedRanges(entryStream, startOffset))

	// If an error occurred when copying
	if err != nil {
		// Return the error
		return
	}

	// Retrieve the resulting buffer from the writter
	result = memoryWriter.WrittenData()

	// Return the result
	return
}

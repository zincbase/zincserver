package main

import (
	"errors"
	"io"
	"os"
)

type DatastoreState struct {
	// The datastore File descriptor
	File *os.File

	// A scheduler object to schedule flushes to the datastore file
	FlushScheduler *DatastoreFlushScheduler

	// A datastore Index allowing timestamp-to-offset lookups
	Index *DatastoreIndex

	// Cached head entry value
	HeadEntryValue *HeadEntryValue

	// A cache object containing the datastore content in parsed form.
	// This is currently used only to cache configuration datastores
	DataCache *VarMap

	// Cached datastore creation time value (= head entry's commit time)
	CreationTime int64
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Head entry operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Read and cache the value and creation time of the head entry,
// always located on range [0:512] of the file
func (this *DatastoreState) LoadHeadEntry() error {
	// Create a new iterator for the datastore
	next := NewEntryStreamIterator(this.File, 0, HeadEntrySize)

	// Iterate once
	iterationResult, err := next()

	// If an error occurred while iterating or the iterator has completed
	if err != nil {
		// Return the error
		return err
	}

	// If no entries were found (most likely the file is empty)
	if iterationResult == nil {
		// Return a invalid head entry error
		return ErrInvalidHeadEntry
	}

	// Test if the first entry is corrupt
	err = iterationResult.VerifyAllChecksums()

	// If the verification failed
	if err != nil {
		// Return the error
		return err
	}

	// Verify the first entry is a valid head entry
	err = iterationResult.VerifyValidHeadEntry()

	// If the verification failed
	if err != nil {
		// Return the error
		return err
	}

	// Read the value of the head entry
	value, err := iterationResult.ReadValue()

	// If an error occurred while reading the value
	if err != nil {
		// Return the error
		return err
	}

	// Deserialize the head entry value and store it in its object
	this.HeadEntryValue = DeserializeHeadEntryValue(value)

	// Store the creation time in its object
	this.CreationTime = iterationResult.Header.CommitTime

	return nil
}

// Update the head entry and persist its updated value to disk
func (this *DatastoreState) UpdateHeadEntry(newValue *HeadEntryValue) (err error) {
	// If the creation time is 0, error
	if this.CreationTime == 0 {
		return errors.New("Creation time is 0")
	}

	// Create a serialized head entry from the cached value and creation time
	serializedHeadEntry := CreateSerializedHeadEntry(newValue, this.CreationTime)

	// Write the serialized head entry to the datastore file at offset 0
	_, err = this.File.WriteAt(serializedHeadEntry, 0)

	// Atomically replace the cached head entry value
	this.HeadEntryValue = newValue

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Data caching operations
///////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DatastoreState) UpdateDataCache(entryStreamReader io.ReaderAt, startOffset int64, endOffset int64) (err error) {
	if this.DataCache == nil {
		this.DataCache = NewEmptyVarMap()
	}
	// Deserialize the given data and append it to the map
	err = DeserializeEntryStreamReaderAndAppendToVarMap(entryStreamReader, startOffset, endOffset, this.DataCache)
	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Misc operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Gets the size of the datastore.
func (this *DatastoreState) GetFileSize() (fileSize int64, err error) {
	// Make sure the datastore is open
	if this.File == nil {
		return 0, ErrDatastoreNotOpen
	}

	// Get the datastore file stat object
	fileInfo, err := this.File.Stat()

	// If an error occured while looking up the file stat
	if err != nil {
		// Return the error
		return
	}

	// Get the file size from the stat object
	fileSize = fileInfo.Size()

	// Return the file size
	return
}

// Returns the total size indexed by this state object
func (this *DatastoreState) Size() int64 {
	return this.Index.TotalSize
}

// Returns the time the datastore was last modified.
func (this *DatastoreState) LastModifiedTime() int64 {
	// Look up the latest update timestamp in the index
	// Note the function would fatally error if the index is nil
	return this.Index.LatestTimestamp()
}

func (this *DatastoreState) Clone() *DatastoreState {
	// Clone the data cache
	var dataCacheClone *VarMap

	if this.DataCache != nil {
		dataCacheClone = this.DataCache.Clone()
	} else {
		dataCacheClone = nil
	}

	return &DatastoreState{
		File:           this.File,
		FlushScheduler: this.FlushScheduler,
		Index:          this.Index.Clone(),
		HeadEntryValue: this.HeadEntryValue,
		DataCache:      dataCacheClone,
		CreationTime:   this.CreationTime,
	}
}

func (this *DatastoreState) Increment() error {
	return FileDescriptors.Increment(this.File)
}

func (this *DatastoreState) Decrement() error {
	return FileDescriptors.Decrement(this.File)
}

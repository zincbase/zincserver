package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"
)

// The datastore operation structure type.
type DatastoreOperations struct {
	// The parent server associated with this datastore
	ParentServer *Server

	// The identifier of the datastore
	Name string

	// The path of the datastore file
	FilePath string

	// A datastore State object
	State *DatastoreState

	// Should this be cached
	IsCached bool

	// A notification source that allows to subscribe to future updates in the datastore
	UpdateNotifier *DatastoreUpdateNotifier

	// An ordered execution lock to serialize write operations
	WriterQueue *ExecQueue

	// A mutex object that is internally used to prevent state read/write races
	stateLock *sync.Mutex
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Construction and initialization
///////////////////////////////////////////////////////////////////////////////////////////////////

// Constructs a new datastore object
func NewDatastoreOperations(datastoreName string, parentServer *Server, isCached bool) *DatastoreOperations {
	return &DatastoreOperations{
		ParentServer: parentServer,

		Name:           datastoreName,
		FilePath:       parentServer.startupOptions.StoragePath + "/" + datastoreName,
		State:          nil,
		IsCached:       isCached,
		UpdateNotifier: NewDatastoreUpdateNotifier(),
		stateLock:      &sync.Mutex{},
		WriterQueue:    NewExecQueue(),
	}
}

// Loads the datastore file and sets the resulting state object as the latest one.
func (this *DatastoreOperations) LoadIfNeeded(increment bool) (*DatastoreState, error) {
	// Lock the state mutex
	this.stateLock.Lock()

	// Unlock the state mutex whenever the function exits
	defer this.stateLock.Unlock()

	// Get existing state object
	existingState := this.State

	// If it exists (I.e. the datastore is already open)
	if existingState != nil {
		// If the returned state's reference count should be incremented
		if increment == true {
			// Increment the current state's file reference count
			existingState.Increment()
		}

		// Return the existing state object
		return existingState, nil
	}
	// Otherwise

	// Start measuring the operation time
	startTime := MonoUnixTimeMilliFloat()

	// Load the datastore
	state, err := this.Load()

	// If an error occurred while loading the datastore
	if err != nil {
		// Return the error
		return nil, err
	}

	// If the returned state's reference count should be incremented
	if increment == true {
		// Increment the current file's reference count
		state.Increment()
	}

	// Set the resulting state object to the current state object
	this.State = state

	// Log a completion message
	this.ParentServer.Logf(1, "Loaded datastore '%s' in %fms", this.Name, MonoUnixTimeMilliFloat()-startTime)

	return state, nil
}

// Loads the datastore. Returns a state object.
func (this *DatastoreOperations) Load() (*DatastoreState, error) {
	var err error

	// Initialize a blank state object
	state := &DatastoreState{
		FlushScheduler: NewDatastoreFlushScheduler(),
	}

	// Open the datastore file
	state.File, err = FileDescriptors.OpenAndIncrement(this.FilePath, os.O_RDWR, 0666)

	// If an error occurred while opening the file
	if err != nil {
		// Return the error
		return nil, err
	}

	// Get file size
	fileSize, err := state.GetFileSize()

	// If an error occurred while getting the file size
	if err != nil {
		// Close the file
		state.Decrement()

		// Return the error
		return nil, err
	}

	// Create a new index object
	state.Index = NewDatastoreIndex()

	// Add new entries to the index by scanning the datastore file
	err = state.Index.AppendFromEntryStream(NewPrefetchingReaderAt(state.File), 0, fileSize, func(iteratorResult *EntryStreamIteratorResult) error {
		checksumErr := iteratorResult.VerifyAllChecksums()
		if checksumErr != nil {
			return checksumErr
		}

		// If the entry is the first one
		if iteratorResult.Offset == 0 {
			// Verify it is a valid head entry
			headEntryErr := iteratorResult.VerifyValidHeadEntry()
			if headEntryErr != nil {
				return headEntryErr
			}
		}

		// If the current entry is the last one but it doesn't have a transaction end flag
		if iteratorResult.EndOffset() == fileSize && !iteratorResult.HasTransactionEndFlag() {
			// Return an unexpected end of stream error
			return io.ErrUnexpectedEOF
		}

		return nil
	})

	// If an error occurred while appending to the index, or the file was empty
	if err != nil || fileSize == 0 {
		// If file ended unexpectedly, was corrupted or last entry didn't include a transaction end marker
		if fileSize == 0 || err == io.ErrUnexpectedEOF || err == ErrCorruptedEntry || err == ErrInvalidHeadEntry {
			// Log message
			this.ParentServer.Logf(1, "An incomplete or corrupted transcacion found in datastore '%s'. Attempting repair..", this.Name)

			// Attempt to roll back to last succesful transaction
			repairedState, err := this.Repair(state)

			// Release original file (the repaired file would have a different descriptor)
			state.Decrement()

			// If an error occurred while repairing the datastore file
			if err != nil {
				// Return the error
				return nil, err
			}

			// Set the repaired datastore as the loaded datastore
			state = repairedState
		} else { // Otherwise, index creation failed for some other reason
			// Release file
			state.Decrement()

			// Return the error
			return nil, err
		}
	}

	// Load head entry
	err = state.LoadHeadEntry()

	// If some error occurred while loading the head entry
	if err != nil {
		// An error here would be highly unexpected since the datastore has just been checked for corruption
		// which included validation of its head entry.
		this.ParentServer.Logf(1, "Datastore '%s' cannot be opened due to an unexpected error while trying to load its head entry: %s", this.Name, err.Error())

		// Release file
		state.Decrement()

		// Return the error
		return nil, err
	}

	// If this is a cached datastore, load its content to memory
	if this.IsCached {
		// Load and deserialize the file's content
		err = state.UpdateDataCache(state.File, 0, state.Size())

		// If some error occured while trying load the file's content
		if err != nil {
			// Release file
			state.Decrement()

			// Return the error
			return nil, err
		}
	}

	return state, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Read operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Creates a reader to the given datastore state object, starting at the first entry having commit timestamp
// greater than the value given as argument.
func (this *DatastoreOperations) CreateReader(state *DatastoreState, updatedAfter int64) (reader io.Reader, readSize int64, err error) {
	// Use the index to find the offset of the first entry matching the condition
	offset := state.Index.FindOffsetOfFirstEntryUpdatedAfter(updatedAfter)

	// If no such entry was found
	if offset == -1 {
		// Return an empty reader with zero length
		return EmptyReader{}, 0, nil
	}

	// Create a reader for the range between the offset and the total
	// size of the indexed entries (in most cases, this would be the size of the file)
	reader = NewRangeReader(state.File, offset, int64(state.Size()))

	// Calculate the read size as the difference between the total size and the read start offset
	readSize = state.Size() - offset

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Write operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Appends a transaction to the datastore.
func (this *DatastoreOperations) Append(transactionBytes []byte, state *DatastoreState, commitTimestamp int64, flushAfterWrite bool, maxFlushDelay int64) (err error) {
	// Clone the given state into a new state object
	newState := state.Clone()

	// Append the transaction to the file
	_, err = newState.File.WriteAt(transactionBytes, state.Size())

	// If an error occured while writing to the file
	if err != nil {
		// Return the error
		return
	}

	// Update the index with the timestamps and offsets of the new entries
	err = newState.Index.AppendFromBuffer(transactionBytes, nil)

	// If an error occured while updating the index
	if err != nil {
		// Return the error
		return
	}

	// If the datastore should be cached
	if this.IsCached {
		// Update its data cache
		err = newState.UpdateDataCache(bytes.NewReader(transactionBytes), 0, int64(len(transactionBytes)))

		// If an error has occurred while updating the cache, return
		if err != nil {
			return
		}
	}

	// Schedule a flush, if needed.
	if flushAfterWrite {
		this.ScheduleFlushIfNeeded(newState, maxFlushDelay)
	}

	// Atomically replace the current state object with the new state object
	this.ReplaceState(newState)

	// Announce the update
	this.UpdateNotifier.AnnounceUpdate(commitTimestamp)

	return
}

// Rewrites the datastore with new content.
func (this *DatastoreOperations) Rewrite(transactionBytes []byte, commitTimestamp int64) (err error) {
	// Safely replace the datastore file with a creation entry and the new transaction as content
	err = CreateOrRewriteFileSafe(this.FilePath, CreateNewDatastoreReaderFromBytes(transactionBytes, commitTimestamp))

	// If an error occurred while replacing the file
	if err != nil {
		// Return the error
		return
	}

	// Reload the datastore
	newState, err := this.Load()

	// If an error occurred while reloading the datstore
	if err != nil {
		// Return the error
		return
	}

	// Atomically replace the current state object with the new state object
	this.ReplaceState(newState)

	// Announce the update
	this.UpdateNotifier.AnnounceUpdate(commitTimestamp)

	return
}

// Schedules a flush.
// If 'maxDelay' is 0, it flushes immediately (which would effectively provide a "full persistence" mode).
func (this *DatastoreOperations) ScheduleFlushIfNeeded(state *DatastoreState, maxDelay int64) {
	// If a flush is already scheduled
	if state.FlushScheduler.FlushScheduled() {
		// Return immediately
		return
	}

	flushIfNeeded := func() {
		startTime := MonoUnixTimeMilli()

		didFlush, err := state.FlushScheduler.EnsureFlush(state.File, time.Duration(maxDelay)*time.Millisecond)
		if err == nil {
			if !didFlush {
				return
			}

			// Log a success message
			this.ParentServer.Logf(1, "Flushed datastore '%s' %dms after written", this.Name, MonoUnixTimeMilli()-startTime)
		} else { // Otherwise,
			// Log a failure message
			this.ParentServer.Logf(1, "Error flushing datastore '%s'. %s", this.Name, err.Error())
		}
	}

	if maxDelay == 0 {
		flushIfNeeded()
	} else {
		// Increment the reference count to ensure the file isn't closed before or while the goroutine
		// executes
		state.Increment()

		go func() {
			flushIfNeeded()

			// Decrement the reference count
			state.Decrement()
		}()
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Compaction operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Compacts the datastore, if needed.
func (this *DatastoreOperations) CompactIfNeeded(state *DatastoreState, minSize int64, minGrowthRatio float64, minUnusedSizeRatio float64) (bool, error) {
	// Store the start time of the operation
	startTime := MonoUnixTimeMilli()

	// Get the current size of the index
	currentSize := state.Size()

	// Continue only if current size is at least the minimum size to perform compaction checks
	if currentSize < minSize {
		return false, nil
	}

	// Continue only if file size has grown a sufficient amount since last check
	if float64(currentSize) < float64(state.HeadEntryValue.LastCompactionCheckSize)*minGrowthRatio {
		return false, nil
	}

	// Create a key index and add all entries to it
	keyIndex := NewDatastoreKeyIndex()
	err := keyIndex.AddFromEntryStream(NewPrefetchingReaderAt(state.File), 0, currentSize)
	if err != nil {
		return false, err
	}

	// Get compacted size and calculate unused size
	compactedSize := keyIndex.GetCompactedSize()
	unusedSize := currentSize - compactedSize

	// If the compacted size is below the threshold for a file rewrite
	if float64(unusedSize)/float64(currentSize) < minUnusedSizeRatio {
		// Update in-place and persist the updated head entry
		err = state.UpdateHeadEntry(&HeadEntryValue{
			Version:                       this.State.HeadEntryValue.Version,
			LastCompactionTime:            this.State.HeadEntryValue.LastCompactionTime,
			LastCompactionCheckTime:       MonoUnixTimeMicro(),
			LastCompactionCheckSize:       currentSize,
			LastCompactionCheckUnusedSize: unusedSize,
		})

		// Return with any error that occurred, or nil
		return false, err
	}

	// Create a timestamp for the compacted datastore head entry
	compactionTimestamp := MonoUnixTimeMicro()

	// Create a new head entry that preserves the original creation time
	compactedDatastoreHeadEntry := CreateSerializedHeadEntry(&HeadEntryValue{
		Version:                       DatastoreVersion,
		LastCompactionTime:            compactionTimestamp,
		LastCompactionCheckTime:       compactionTimestamp,
		LastCompactionCheckSize:       compactedSize,
		LastCompactionCheckUnusedSize: 0,
	}, state.CreationTime)

	// Create a reader for the compacted datastore
	compactedDatastoreReader := io.MultiReader(
		bytes.NewReader(compactedDatastoreHeadEntry),
		keyIndex.CreateReaderForCompactedRanges(state.File, HeadEntrySize))

	// Rewrite the file with the compacted ranges
	err = CreateOrRewriteFileSafe(this.FilePath, compactedDatastoreReader)

	// If an error occurred while rewriting the file
	if err != nil {
		// Return the error
		return false, err
	}

	// Reload the datastore
	newState, err := this.Load()

	// If an error occurred while loading the rewritten file
	if err != nil {
		// Return the error
		return false, err
	}

	// Atomically replace the current state object with the new state object
	this.ReplaceState(newState)

	// Log message
	this.ParentServer.Logf(1, "Compacted datastore '%s' from %d to %d bytes in %dms", this.Name, currentSize, compactedSize, MonoUnixTimeMilli()-startTime)

	// Return without error
	return true, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// State related operations
///////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DatastoreOperations) ReplaceState(newState *DatastoreState) (err error) {
	this.stateLock.Lock()
	defer this.stateLock.Unlock()

	oldState := this.State
	this.State = newState

	if oldState != nil {
		if newState == nil || newState.File.Fd() != oldState.File.Fd() {
			err = oldState.Decrement()
		}

		if err != nil {
			return
		}
	}

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Cleanup and destruction operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Destroys the datastore, but not its configuration.
func (this *DatastoreOperations) Destroy() (err error) {
	// Delete the datastore file
	err = UnlinkFileSafe(this.FilePath)

	// If an error occurred during the operation
	if err != nil {
		// Return the error
		return
	}

	// Close
	this.Close()

	// Announce the operation
	this.UpdateNotifier.AnnounceUpdate(math.MaxInt64)

	return
}

// Closes the currently associated datastore state object.
func (this *DatastoreOperations) Close() (err error) {
	return this.ReplaceState(nil)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Repair operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Tries rolling back the datastore to the last successful transaction.
func (this *DatastoreOperations) Repair(state *DatastoreState) (*DatastoreState, error) {
	// Get the size of the datastore file
	originalSize, err := state.GetFileSize()

	// If an error occurred while getting the size
	if err != nil {
		// Return the error
		return nil, err
	}

	// Scan the file to get a safe truncation size
	repairedSize, err := FindSafeTruncationSize(NewPrefetchingReaderAt(state.File), originalSize)

	// If an error occurred while checking for a truncation size
	if err != nil {
		// Return the error
		return nil, err
	}

	// Create a backup copy of the corrupted datastore
	backupFilePath := fmt.Sprintf("%s.corrupted-%d", this.FilePath, MonoUnixTimeMicro())
	err = CreateOrRewriteFileSafe(backupFilePath, state.File)

	// If an error occurred while creating a backup file
	if err != nil {
		// Log a message
		this.ParentServer.Logf(1, "Error while creating a backup of corrupted datastore '%s': %s.", this.Name, err.Error())

		// Return the error
		return nil, err
	}

	if repairedSize == 0 {
		err = CreateOrRewriteFileSafe(this.FilePath, CreateNewDatastoreReaderFromBytes([]byte{}, MonoUnixTimeMicro()))
	} else {
		err = CreateOrRewriteFileSafe(this.FilePath, NewRangeReader(state.File, 0, repairedSize))
	}

	// If an error occurred while rewriting the file
	if err != nil {
		// Log a message
		this.ParentServer.Logf(1, "Error while repairing datastore '%s': %s", this.Name, err.Error())

		// Return the error
		return nil, err
	}

	// Reload the datastore
	newState, err := this.Load()

	// If an error occurred while recreating the index
	if err != nil {
		// Log a failure message
		this.ParentServer.Logf(1, "Failed to reload datastore '%s' after repair.", this.Name)
	} else { // Otherwise
		// Log a success message
		this.ParentServer.Logf(1, "Repaired datastore '%s'. Original size %d bytes, Repaired size %d bytes. A backup of the corrupted datastore file has been saved to '%s'.", this.Name, originalSize, repairedSize, backupFilePath)
	}

	return newState, err
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Misc operations
///////////////////////////////////////////////////////////////////////////////////////////////////
// Gets a timestamp and ensures that it's strictly greater than the last modification time.
// In the rare case the current time is equal to or less than the last modification time,
// it would wait as much as needed until that time has passed.
// If the last modification time is far in the future, such that the system is running with severly inaccurate
// clock, the function may effectively stall for a long time.
func (this *DatastoreOperations) GetCollisionFreeTimestamp(state *DatastoreState) (timestamp int64) {
	timestamp = MonoUnixTimeMicro()

	// If a state wasn't given, return with the new timestamp, without checking for collisions.
	// This may happen if the datastore is being rewritten.
	if state == nil {
		return
	}

	// Get the last modified time for the datastore
	lastModifiedTime := state.LastModifiedTime()

	// Check if timestamp is less than the last modified time
	if timestamp < lastModifiedTime { // if it is strictly less than last modified time
		// Calculate the needed sleep time, convert microseconds to floating point milliseconds
		sleepTimeMilli := float64(lastModifiedTime-timestamp+1) / 1000

		// Log a message
		this.ParentServer.Logf(1, "The last modification time of datastore '%s' is greater than current time. Sleeping for %f.3ms until the anomaly is resolved..", this.Name, sleepTimeMilli)

		// Sleep until timestamp is strictly greater than the last modification time
		Sleep(sleepTimeMilli)

		// Get a new timestamp
		timestamp = MonoUnixTimeMicro()
	} else if timestamp == lastModifiedTime { // If it equals exactly
		// Spinwait, until the new timestamp is strictly greater than previous one
		for timestamp == lastModifiedTime {
			// Get a new timestamp
			timestamp = MonoUnixTimeMicro()
		}
	}

	// Return the resulting timestamp
	return
}

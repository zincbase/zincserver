package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	//"log"
	"errors"
)

// The datastore operation structure type.
type DatastoreOperationsEntry struct {
	// The parent server associated with this datastore
	parentServer *Server

	// The identifier of the datastore
	name string

	// The path of the datastore file
	filePath string

	// The datastore file descriptor, if open, otherwise nil
	file *os.File

	// An datastore index allowing timestamp-to-offset lookups
	index *DatastoreIndex

	// A notification source that allows to subscribe to future updates in the datastore
	updateNotifier *DatastoreUpdateNotifier

	// An object tracking the rate and type of operations performed by each client, allowing to set
	// limits for this datastore
	rateLimiter *RequestRateLimiter

	// A flag that signifies if flush operation is currently scheduled
	flushScheduled bool

	// Cached head entry value
	headEntryValue *HeadEntryValue

	// Datastore creation time
	creationTime int64

	// A cache object containing the datastore content in parsed form.
	// This is currently used only to cache configuration datastores
	dataCache *VarMap

	// The associated configuration datastore operations object. For the configuration datastores
	// themselves, this would always be nil.
	configDatastore *DatastoreOperationsEntry

	// A mutex object that is internally used to prevent datastore initialization races (LoadIfNeeded)
	initializationMutex *sync.Mutex

	// A mutex object that is internally used to synchronize flushing operations
	flushMutex *sync.Mutex

	// A reader-writer mutex for this datastore. Should only be used by consumers, not internally.
	sync.RWMutex
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Construction and initialization
///////////////////////////////////////////////////////////////////////////////////////////////////

// Constructs a new datastore object.
func NewDatastoreOperationsEntry(datastoreName string, parentServer *Server) *DatastoreOperationsEntry {
	return &DatastoreOperationsEntry{
		parentServer: parentServer,

		name:                datastoreName,
		filePath:            parentServer.startupOptions.StoragePath + "/" + datastoreName,
		file:                nil,
		initializationMutex: &sync.Mutex{},
		flushMutex:          &sync.Mutex{},
		index:               nil,
		headEntryValue:      nil,
		creationTime:        0,
		updateNotifier:      NewDatastoreUpdateNotifier(),
		configDatastore:     nil,

		dataCache:   nil,
		rateLimiter: NewRequestRateLimiter(),
	}
}

// Loads the datastore, if needed
func (this *DatastoreOperationsEntry) LoadIfNeeded() (err error) {
	// Lock using initialization mutex
	this.initializationMutex.Lock()

	// Unlock whenever the function exists
	defer this.initializationMutex.Unlock()

	// Check if the datastore is already open (thread-safe)
	if this.file != nil {
		// In that case, return
		return
	}

	// Start measuring the operation time
	startTime := MonoUnixTimeMilliFloat()

	// Open the datastore file
	this.file, err = FileDescriptors.OpenAndIncrement(this.filePath, os.O_RDWR, 0666)

	// If an error occurred while opening the file
	if err != nil {
		// Return the error
		return
	}

	// Get file size
	fileSize, err := this.GetFileSize()

	// If an error occurred while getting the file size
	if err != nil {
		// Release the datastore
		this.Release()

		// Return the error
		return
	}

	// Create index
	this.index = NewDatastoreIndexWithFullChecksumVerification()
	err = this.index.AddFromEntryStream(NewPrefetchingReaderAt(this.file), 0, fileSize)

	// If an error occurred when creating the index or the file was empty
	if err != nil || fileSize == 0 {
		// If file ended unexpectedly, was corrupted or last entry didn't include a transaction end marker
		if err == io.ErrUnexpectedEOF || err == ErrCorruptedEntry || fileSize == 0 {
			// Log message
			this.parentServer.Log(fmt.Sprintf("An incomplete or corrupted transcacion found in datastore '%s'. Attempting repair..", this.name), 1)

			// Attempt to roll back to last succesful transaction, this would also attempt to
			// reload the index if the repair operation has succeeded
			err = this.RepairIfNeeded()

			// If an error occurred while repairing the datastore file
			if err != nil {
				// Release
				this.Release()

				// Return the error
				return
			}

			// Get file size again
			fileSize, err = this.GetFileSize()

			// If an error occurred while getting the file size
			if err != nil {
				// Release
				this.Release()

				// Return the error
				return
			}
		} else { // Otherwise, index creation failed for some other reason
			// Release
			this.Release()

			// Return the error
			return
		}
	}

	// Load head entry
	err = this.loadHeadEntry()

	// If some error occured while loading the head entry
	if err != nil {
		// Log a message
		if err == io.ErrUnexpectedEOF || err == ErrInvalidHeadEntry || err == ErrCorruptedEntry {
			this.parentServer.Log(fmt.Sprintf("Datastore '%s' cannot be opened as it has an invalid, missing or corrupted head entry", this.name), 1)
		} else {
			this.parentServer.Log(fmt.Sprintf("Datastore '%s' cannot be opened due to an unexpected error while trying to load its head entry: %s", this.name, err), 1)
		}

		// Release and return the error
		this.Release()
		return
	}

	// If this is a configuration datastore, cache its content
	if this.IsConfig() {
		var updatedDataCache *VarMap

		// Load and deserialize the file's content
		updatedDataCache, err = this.CreateUpdatedDataCache(this.file, 0, fileSize)

		// If some error occured while trying load the file's content
		if err != nil {
			// Release
			this.Release()

			// Return the error
			return
		}

		// Atomically replace the data cache with the new one
		this.dataCache = updatedDataCache
	} else { // Otherwise
		// Load corresponding configuration datastore, if needed
		err = this.configDatastore.LoadIfNeeded()

		// If some error occured while trying load the configuration datastore
		if err != nil {
			switch err.(type) {
			case *os.PathError:
				// If the error was a "not found" error, that's OK, it means there simply
				// isn't a configuration datastore for this datastore.
				// set the error to 'nil', and continue
				err = nil
			default: // Otherwise
				// Rrelease
				this.Release()

				// Rreturn the error
				return
			}
		}
	}

	// Log a completion message to console
	this.parentServer.Log(fmt.Sprintf("Loaded datastore '%s' in %fms", this.name, MonoUnixTimeMilliFloat()-startTime), 1)
	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Read operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Creates a reader to the datastore, starting at the first entry with commit timestamp
// greater than the value given as argument.
func (this *DatastoreOperationsEntry) CreateReader(updatedAfter int64) (reader io.Reader, readSize int64, err error) {
	// Make sure the datastore is open
	if this.file == nil {
		return nil, 0, ErrDatastoreNotOpen
	}

	// Use the index to find the offset of the first entry matching the condition
	offset := this.index.FindOffsetOfFirstEntryUpdatedAfter(updatedAfter)

	// If no such entry was found
	if offset == -1 {
		// Return an empty reader with zero length
		return EmptyReader{}, 0, nil
	}

	// Create a reader for the range between the offset and the total
	// size of the indexed entries (in most cases, this would be the size of the file)
	reader = NewRangeReader(this.file, offset, int64(this.index.TotalSize))
	readSize = this.index.TotalSize - offset

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Write operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Commits a transaction, which is given as a stream of serialized entries. The transaction is verified and
// each one of its entries is stamped with new commit timestamp. The last entry is also added a transaction
// end flag.
func (this *DatastoreOperationsEntry) CommitTransaction(transactionBytes []byte) (commitTimestamp int64, err error) {
	// Make sure the datastore is open
	if this.file == nil {
		return 0, ErrDatastoreNotOpen
	}

	// If the transaction is empty, return without error
	if len(transactionBytes) == 0 {
		return
	}

	// Get the datastore size limit
	datastoreSizeLimit, _ := this.GetInt64ConfigValue("['datastore']['limit']['maxSize']")

	// Make sure the transaction wouldn't cause it to exceed this limit
	if datastoreSizeLimit > 0 && this.index.TotalSize+int64(len(transactionBytes)) > datastoreSizeLimit {
		return 0, ErrDatastoreTooLarge{fmt.Sprintf("Datastore '%s' is limited to a maximum size of %d bytes", this.name, datastoreSizeLimit)}
	}

	// Get commit timestamp
	commitTimestamp = this.GetColisionFreeTimestamp()

	// Validate and prepare transaction: rewrite its commit timestamps and ensure transaction end mark
	// for last entry
	err = ValidateAndPrepareTransaction(transactionBytes, commitTimestamp)
	if err != nil {
		return
	}

	// If this datastore should be cached
	var updatedDataCache *VarMap

	if this.IsCached() {
		// Get an updated data cache value that would replace the old cache value,
		// once all write operations have completed successfully
		updatedDataCache, err = this.CreateUpdatedDataCache(bytes.NewReader(transactionBytes), 0, int64(len(transactionBytes)))

		// If an error has occurred while loading the cache, return
		if err != nil {
			return
		}
	}

	// Write the transaction to the file
	_, err = this.file.WriteAt(transactionBytes, int64(this.index.TotalSize))

	// If an error occured when writing to the file, return
	if err != nil {
		return
	}

	// Update the index with the timestamps and offsets of the new entries
	err = this.index.AppendFromBuffer(transactionBytes)

	// If an error occured while updating the index, return
	if err != nil {
		return
	}

	// Perform a compaction check and compact if needed
	compacted, err := this.CompactIfNeeded()

	// If an error occurred while compacting, return
	if err != nil {
		return
	}

	// If compaction was not performed, schedule a flush, if needed.
	if !compacted {
		this.ScheduleFlushIfNeeded()
	}

	// Now that data has been successfuly written to the file system, update cache if needed
	if updatedDataCache != nil {
		this.dataCache = updatedDataCache
	}

	// Announce the update
	this.updateNotifier.AnnounceUpdate(commitTimestamp)

	return
}

// Rewrites the datastore with the new content, applies similar processing to CommitTransaction before
// writing the given data.
func (this *DatastoreOperationsEntry) Rewrite(transactionBytes []byte) (commitTimestamp int64, err error) {
	// Note: no need to check if the datastore is open here, this should succeed even if it is closed

	// Get the datastore size limit
	datastoreSizeLimit, _ := this.GetInt64ConfigValue("['datastore']['limit']['maxSize']")

	// Make sure the transaction wouldn't cause it to exceed this limit
	if datastoreSizeLimit > 0 && int64(len(transactionBytes)) > datastoreSizeLimit {
		return 0, ErrDatastoreTooLarge{fmt.Sprintf("Datastore '%s' is limited to a maximum size of %d bytes", this.name, datastoreSizeLimit)}
	}

	// Get a safe commit timestamp (must be strictly greater than a previous commit timestamp)
	commitTimestamp = this.GetColisionFreeTimestamp()

	// Validate and prepare transaction: rewrite its commit timestamps and ensure transaction end mark
	// for last entry
	err = ValidateAndPrepareTransaction(transactionBytes, commitTimestamp)

	// If an error occured when validating the transaction
	if err != nil {
		// Return it
		return
	}

	var updatedDataCache *VarMap

	// If this datastore should be cached
	if this.IsCached() {
		// Get an updated data cache value that would replace the old cache value
		// once all write operations have successfuly completed
		updatedDataCache, err = this.CreateUpdatedDataCache(bytes.NewReader(transactionBytes), 0, int64(len(transactionBytes)))

		// If an error occurred when creating the updated cache value
		if err != nil {
			// Return the error
			return
		}
	}

	// Close the file and release all resources
	err = this.Release()

	// If an error occurred when releasing the datastore
	if err != nil {
		// Return the error
		return
	}

	// Safely replace file the datastore file with a creation entry and the new transaction as content
	err = ReplaceFileSafely(this.filePath, CreateNewDatastoreReaderFromBytes(transactionBytes, commitTimestamp))

	// If an error occured when replacing the file
	if err != nil {
		// Return the error
		return
	}

	// Now that data has been written to the file system, update the data cache if needed
	if updatedDataCache != nil {
		this.dataCache = updatedDataCache
	}

	// Announce the update
	this.updateNotifier.AnnounceUpdate(commitTimestamp)
	return
}

// Schedules a flush if the datastore is configured to invoke it.
// If the 'maxDelay' setting is set to 0, it flushes immediately
// (This setting would effectively provide a 'full persistence' mode).
func (this *DatastoreOperationsEntry) ScheduleFlushIfNeeded() {
	// If a flush is already scheduled, return immediately
	if this.flushScheduled {
		return
	}

	// Get the flush setting for this datastore
	flushEnabled, err := this.GetBoolConfigValue("['datastore']['flush']['enabled']")

	// If the operation failed or flush is disabled, return without error
	if err != nil || flushEnabled == false {
		return
	}

	// Get the maximum delay value for flushes
	maxDelayToFlush, err := this.GetInt64ConfigValue("['datastore']['flush']['maxDelay']")

	// If no matching key was found or an invalid flush delay is specified
	// return without error
	if err != nil || maxDelayToFlush < 0 {
		return
	}

	// Define a local function that will perorm the flush operation
	flush := func(file *os.File) {
		// Store the start time of the operation
		startTime := MonoUnixTimeMilli()

		// Call the appropriate OS `sync` function
		err := file.Sync()

		// If no error while executing the operation
		if err == nil {
			// Log a success message to the console
			this.parentServer.Log(fmt.Sprintf("Flushed datastore '%s' in %dms", this.name, MonoUnixTimeMilli()-startTime), 1)
		} else {
			// Otherwise log a failure message to the console
			this.parentServer.Log(fmt.Sprintf("Error flushing datastore '%s'. %s", this.name, err.Error()), 1)
		}
	}

	// If a zero delay is defined
	if maxDelayToFlush == 0 {
		// Flush immediately
		flush(this.file)

		// Then return
		return
	}

	// Otherwise, a greater-than-zero flush time is defined

	// Set a flag to signify a flush is currently scheduled
	this.flushScheduled = true

	// Store the file descriptor in a local variable
	targetFile := this.file

	// Increment the file descriptor, to make sure it isn't released
	FileDescriptors.Increment(targetFile)

	// Continue in a new goroutine
	go func() {
		// If the specified delay is larger than zero
		if maxDelayToFlush > 0 {
			// Wait until it's over
			Sleep(maxDelayToFlush)
		}

		// Acquire a lock for the flush
		this.flushMutex.Lock()

		// Defer this lock to be released once the function has completed
		defer this.flushMutex.Unlock()

		// Disable the flush scheduled flag
		this.flushScheduled = false

		// Perform the flush using the helper function
		flush(targetFile)

		// Decrement the file descriptor
		FileDescriptors.Decrement(targetFile)
	}()
}

// Takes the given entry stream, deserializes it and non-destructively appends it
// to the current cache of the datastore's content and returns the result.
func (this *DatastoreOperationsEntry) CreateUpdatedDataCache(entryStreamReader io.ReaderAt, startOffset int64, endOffset int64) (updatedCache *VarMap, err error) {
	// Check if a cached variable already exists
	if this.dataCache == nil {
		// If it doesn't, use a new empty map
		updatedCache = NewEmptyVarMap()
	} else {
		// If it does, create a clone of it
		updatedCache = this.dataCache.Clone()
	}

	// Deserialize the given data and append it to the map
	err = DeserializeEntryStreamReaderAndAppendToVarMap(entryStreamReader, startOffset, endOffset, updatedCache)

	// If an error occured during the operation
	if err != nil {
		// Return the error
		return nil, err
	}

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Compaction operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Compacts the datastore, if needed.
func (this *DatastoreOperationsEntry) CompactIfNeeded() (bool, error) {
	// Store the start time of the operation
	startTime := MonoUnixTimeMilli()

	// Get the current size of the index
	currentSize := this.index.TotalSize

	// Read related configuration options for compaction
	compactionEnabled, _ := this.GetBoolConfigValue("['datastore']['compaction']['enabled']")
	if compactionEnabled == false {
		return false, nil
	}
	compactionMinSize, err1 := this.GetInt64ConfigValue("['datastore']['compaction']['minSize']")
	compactionMinGrowthRatio, err2 := this.GetFloat64ConfigValue("['datastore']['compaction']['minGrowthRatio']")
	compactionMinUnusedSizeRatio, err3 := this.GetFloat64ConfigValue("['datastore']['compaction']['minUnusedSizeRatio']")
	if err1 != nil || err2 != nil || err3 != nil {
		return false, nil
	}

	// Continue only if current size is at least the minimum size to perform compaction checks
	if currentSize < compactionMinSize {
		return false, nil
	}

	// Continue only if file size has grown a sufficient amount since last check
	if float64(currentSize) < float64(this.headEntryValue.LastCompactionCheckSize)*compactionMinGrowthRatio {
		return false, nil
	}

	// Create a key index and add all entries to it
	keyIndex := NewDatastoreKeyIndex()
	err := keyIndex.AddFromEntryStream(NewPrefetchingReaderAt(this.file), 0, currentSize)
	if err != nil {
		return false, err
	}

	// Get compacted size and calculate unused size
	compactedSize := keyIndex.GetCompactedSize()
	unusedSize := currentSize - compactedSize

	// If the compacted size is below the threshold for a file rewrite
	if float64(unusedSize)/float64(currentSize) < compactionMinUnusedSizeRatio {

		// Update the head entry for latest compaction check results
		this.headEntryValue.LastCompactionCheckTime = MonoUnixTimeMicro()
		this.headEntryValue.LastCompactionCheckSize = currentSize
		this.headEntryValue.LastCompactionCheckUnusedSize = unusedSize

		// Store the updated head entry
		err = this.storeHeadEntry()

		// If an error has occured while storing the updated head entry
		if err != nil {
			// Return the error
			return false, err
		}

		// Return with no error
		return false, nil
	}

	// Create a timestamp for the compacted datastore head entry
	compactionTimestamp := MonoUnixTimeMicro()

	// Create a new head entry that preserves the existing creation time
	compactedDatastoreHeadEntry := CreateSerializedHeadEntry(&HeadEntryValue{
		Version:                       DatastoreVersion,
		LastCompactionTime:            compactionTimestamp,
		LastCompactionCheckTime:       compactionTimestamp,
		LastCompactionCheckSize:       compactedSize,
		LastCompactionCheckUnusedSize: 0,
	}, this.creationTime)

	// Create a reader for the compacted datastore
	compactedDatastoreReader := io.MultiReader(
		bytes.NewReader(compactedDatastoreHeadEntry),
		keyIndex.CreateReaderForCompactedRanges(this.file, HeadEntrySize))

	// Rewrite the file with the compacted ranges
	err = ReplaceFileSafely(this.filePath, compactedDatastoreReader)

	// If an error occurred while rewriting the file
	if err != nil {
		// Return the error
		return false, err
	}

	// Release datastore resources
	err = this.Release()

	// If an error has occurred when releasing the datastore
	if err != nil {
		// Return the error
		return false, err
	}

	// Log message
	this.parentServer.Log(fmt.Sprintf("Compacted datastore '%s' from %d to %d bytes in %dms", this.name, currentSize, compactedSize, MonoUnixTimeMilli()-startTime), 1)

	return true, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Cleanup and destruction operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Releases the file descriptor and clears all in-memory datastore resources.
func (this *DatastoreOperationsEntry) Release() (err error) {
	// If the datastore file isn't currently open
	if this.file == nil {
		// Return with no error
		return nil
	}

	// Decrement the file descriptor counter
	err = FileDescriptors.Decrement(this.file)

	// If an error occured when decrementing the counter
	if err != nil {
		// Return the error
		return
	}

	// Clear file object
	this.file = nil

	// Clear index
	this.index = nil

	// Clear cached head entry value
	this.headEntryValue = nil

	// Clear creation time
	this.creationTime = 0

	// The cached data shouldn't be cleared here
	// Otherwise configuration would become nil every time the datastore is rewritten
	// or an error occurs:
	//this.dataCache = nil

	return
}

// Destroys the datastore, but not its configuration.
func (this *DatastoreOperationsEntry) Destroy() (err error) {
	// Release the datastore
	err = this.Release()

	// If an error occurred when releasing the datastore
	if err != nil {
		// Return the error
		return
	}

	// Delete the datastore file
	err = DeleteFileSafely(this.filePath)

	// If an error occurred during the operation
	if err != nil {
		// Return the error
		return
	}

	// The cached content is only cleared once the datastore has been successfuly
	// destroyed
	this.dataCache = nil

	// Announce the operation
	this.updateNotifier.AnnounceUpdate(math.MaxInt64)

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Repair operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Tries rolling back the datastore to the last successful transaction.
func (this *DatastoreOperationsEntry) RepairIfNeeded() (err error) {
	// Get the size of the datastore file
	originalSize, err := this.GetFileSize()

	// If an error occurred when getting the size
	if err != nil {
		// Return the error
		return
	}

	// Scan the file to get a safe truncation size
	repairedSize, err := FindSafeTruncationSize(NewPrefetchingReaderAt(this.file), originalSize)

	// If an error occurred when checking for a truncation size
	if err != nil {
		// Return the error
		return
	}

	// If the truncated datastore size is equal to the current size of the datastore
	if repairedSize > 0 && repairedSize == originalSize {
		// No need to repair anything
		this.parentServer.Log(fmt.Sprintf("No need to repair datastore '%s'.", this.name), 1)
		return
	}

	// Create a backup copy of the corrupted datastore
	backupFilePath := fmt.Sprintf("%s.corrupted-%d", this.filePath, MonoUnixTimeMicro())
	err = ReplaceFileSafely(backupFilePath, this.file)

	// If an error occurred when creating a backup file
	if err != nil {
		// Log a message
		this.parentServer.Log(fmt.Sprintf("Error while creating a backup of corrupted datastore '%s': %s.", this.name, err), 1)

		// Return the error
		return
	}

	// Truncate the datastore file
	err = this.file.Truncate(repairedSize)

	// If an error occurred when truncating the file
	if err != nil {
		// Log a message
		this.parentServer.Log(fmt.Sprintf("Error while reparing datastore '%s': %s", this.name, err), 1)

		// Return the error
		return
	}

	// Seek the files to its new end offset
	_, err = this.file.Seek(repairedSize, 0)

	// If an error occurred while seeking the file
	if err != nil {
		// Log a message
		this.parentServer.Log(fmt.Sprintf("Error while repairing datastore '%s': %s", this.name, err), 1)

		// Return the error
		return
	}

	// Check if the datastore file is now empty
	if repairedSize == 0 {
		// Add a new head entry to the file
		_, err = io.Copy(this.file, CreateNewDatastoreReaderFromBytes([]byte{}, MonoUnixTimeMicro()))

		// If some error occured while trying to recreate the file
		if err != nil {
			// Return the error
			return
		}

		// Set the repaired size to the size of the head entry
		repairedSize = HeadEntrySize
	}

	// Try recreating the index up to the repaired size
	this.index = NewDatastoreIndexWithFullChecksumVerification()
	err = this.index.AddFromEntryStream(this.file, 0, repairedSize)

	// If an error occurred when recreating the index
	if err != nil {
		// Log a message
		this.parentServer.Log(fmt.Sprintf("Failed to recreate index for datastore '%s' after repair.", this.name), 1)
	} else { // Otherwise
		// Log a message
		this.parentServer.Log(fmt.Sprintf("Repaired datastore '%s'. Original size %d bytes, Repaired size %d bytes. A backup of the corrupted datastore file has been saved to '%s'.", this.name, originalSize, repairedSize, backupFilePath), 1)
	}

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Head entry operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Cache the value and creation time of the head entry, always located on range [0:512] of the file
func (this *DatastoreOperationsEntry) loadHeadEntry() error {
	// Create a new iterator for the datastore
	iterate := NewEntryStreamIterator(this.file, 0, HeadEntrySize)

	// Iterate once
	iterationResult, err := iterate()

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

	// If the first entry is corrupt
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
	this.headEntryValue = DeserializeHeadEntryValue(value)

	// Store the creation time in its object
	this.creationTime = iterationResult.PrimaryHeader.CommitTime

	return nil
}

// Persist the head entry object to disk
func (this *DatastoreOperationsEntry) storeHeadEntry() (err error) {
	// If the cached head entry value doesn't exist, error
	if this.headEntryValue == nil {
		return errors.New("No head entry is not loaded")
	}

	// If the creation time is 0, error
	if this.creationTime == 0 {
		return errors.New("Creation time is 0")
	}

	// Create a serialized head entry from the cached value and creation time
	serializedHeadEntry := CreateSerializedHeadEntry(this.headEntryValue, this.creationTime)

	// Write the serialized head entry to the datastore file at offset 0
	_, err = this.file.WriteAt(serializedHeadEntry, 0)

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Configuration lookup operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Gets a string typed configuration value.
func (this *DatastoreOperationsEntry) GetStringConfigValue(key string) (value string, err error) {
	// If a datastore-specific configuration datastore is available
	if this.configDatastore != nil {
		// Get its cached object
		cachedDatastoreConfig := this.configDatastore.dataCache

		// If its cached object exists
		if cachedDatastoreConfig != nil {
			// Lookup the value for the given key
			value, err = cachedDatastoreConfig.GetString(key)

			// If the key was found
			if err == nil {
				// Return its value
				return
			}
		}
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.parentServer.GlobalConfig().GetString(key)
}

// Gets a boolean typed configuration value.
func (this *DatastoreOperationsEntry) GetBoolConfigValue(key string) (value bool, err error) {
	// If a datastore-specific configuration datastore is available
	if this.configDatastore != nil {
		// Get its cached object
		cachedDatastoreConfig := this.configDatastore.dataCache

		// If its cached object exists
		if cachedDatastoreConfig != nil {
			// Lookup the value for the given key
			value, err = cachedDatastoreConfig.GetBool(key)

			// If the key was found
			if err == nil {
				// Return the value
				return
			}
		}
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.parentServer.GlobalConfig().GetBool(key)
}

// Gets a 64-bit integer typed configuration value.
func (this *DatastoreOperationsEntry) GetInt64ConfigValue(key string) (value int64, err error) {
	// If a datastore-specific configuration datastore is available
	if this.configDatastore != nil {
		// Get its cached object
		cachedDatastoreConfig := this.configDatastore.dataCache

		// If its cached object exists
		if cachedDatastoreConfig != nil {
			// Lookup the value for the given key
			value, err = cachedDatastoreConfig.GetInt64(key)

			// If the key was found
			if err == nil {
				// Return the value
				return
			}
		}
	}

	// Otherwise, look up the key in the global configuration and return its value if found
	return this.parentServer.GlobalConfig().GetInt64(key)
}

// Gets a 64-bit float typed configuration value.
func (this *DatastoreOperationsEntry) GetFloat64ConfigValue(key string) (value float64, err error) {
	// If a datastore-specific configuration datastore is available
	if this.configDatastore != nil {
		// Get its cached object
		cachedDatastoreConfig := this.configDatastore.dataCache

		// If its cached object exists
		if cachedDatastoreConfig != nil {
			// Lookup the value for the given key
			value, err = cachedDatastoreConfig.GetFloat64(key)

			// If the key was found
			if err == nil {
				return
			}
		}
	}

	return this.parentServer.GlobalConfig().GetFloat64(key)
}

// Gets the size of the datastore.
func (this *DatastoreOperationsEntry) GetFileSize() (fileSize int64, err error) {
	// Make sure the datastore is open
	if this.file == nil {
		return 0, ErrDatastoreNotOpen
	}

	// Get the datastore file stat object
	fileInfo, err := this.file.Stat()

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

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Misc operations
///////////////////////////////////////////////////////////////////////////////////////////////////

// Returns the time the datastore was last modified.
func (this *DatastoreOperationsEntry) LastModifiedTime() int64 {
	// Look up the latest update timestamp in the index
	// Note the function would fatally error if the index is nil
	return this.index.LatestTimestamp()
}

// Checks if this is a configuration datastore.
func (this *DatastoreOperationsEntry) IsConfig() bool {
	// Check if the filename has the suffix '.config'
	return strings.HasSuffix(this.name, ".config")
}

// Checks if this is the global configuration datastore.
func (this *DatastoreOperationsEntry) IsGlobalConfig() bool {
	// Check if the filename is exactly ".config"
	return this.name == ".config"
}

// Checks if this datastore should be cached in memory.
func (this *DatastoreOperationsEntry) IsCached() bool {
	// Return based on whether this is a configuration datastore
	return this.IsConfig()
}

// Gets a timestamp and ensures that it's strictly greater than the latest update time.
// In the rare case the current time is equal to or less than the latest update time,
// it would wait as much as needed until that time has passed.
// If the latest update time is far in the future, such that the system is running with severly inaccurate
// clock, the function may effectively stall for a very long time.
func (this *DatastoreOperationsEntry) GetColisionFreeTimestamp() (timestamp int64) {
	timestamp = MonoUnixTimeMicro()

	// If an index is not available, return with new timestamp
	// This may happen if the datastore is being rewritten
	if this.index == nil {
		return
	}

	// Get the last modified time for the datastore
	lastModifiedTime := this.LastModifiedTime()

	// Check if timestamp is less than or equals last modified timestamp
	if timestamp == lastModifiedTime { // If it equals exactly
		// Spinwait, until the new timestamp is strictly greater than previous one
		for timestamp == lastModifiedTime {
			// Get new timestamp
			timestamp = MonoUnixTimeMicro()
		}
	} else if timestamp < lastModifiedTime { // if it is strictly less than last modified time
		// Calculate the needed sleep time
		sleepTime := lastModifiedTime - timestamp + 1

		// Log a message
		this.parentServer.Log(fmt.Sprintf("The last modification time of datastore '%s' is greater than current time. Sleeping for %dms until the anomaly is resolved..", this.name, sleepTime), 1)

		// Sleep until timestamp is strictly greater than the last modification time
		Sleep(sleepTime)

		// Get new timestamp
		timestamp = MonoUnixTimeMicro()
	}

	return
}

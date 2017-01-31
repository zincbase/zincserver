package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	//"log"
)

type DatastoreOperationsEntry struct {
	// The parent server associated with this datastore
	parentServer *Server
	
	// The identifier of the datastore
	name string

	// The path of the datastore file
	filePath            string
	
	// The datastore file descriptor, if open, otherwise nil
	file                *os.File

	// An datastore index allowing timestamp-to-offset lookups
	index           *DatastoreIndex	

	// A notification source that allows to subscribe to future updates in the datastore
	updateNotifier  *DatastoreUpdateNotifier

	// An object tracking the rate and type of operations performed by each client, allowing to set
	// limits for this datastore
	rateLimiter *RequestRateLimiter

	// A flag that signifies if flush operation is currently scheduled
	flushScheduled      bool

	// Cache object holding the compaction metadata file content
	compactionState *DatastoreCompactionState

	// A cache object containing the datastore content in parsed form.
	// This is currently used only to cache configuration datastores
	dataCache   *VarMap

	// The associated configuration datastore opreations object. For the configuration datastores
	// themselves, this would always be nil.
	configDatastore *DatastoreOperationsEntry

	// A mutex object that is internally used to prevent datastore initialization races (LoadIfNeeded)
	initializationMutex *sync.Mutex

	// A mutex object that is internally used to synchronize flushing operations
	flushMutex          *sync.Mutex

	// A reader-writer mutex for this datastore. Should only be used by consumers, not internally.
	sync.RWMutex
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Initialization
///////////////////////////////////////////////////////////////////////////////////////////////////
func (this *DatastoreOperationsEntry) LoadIfNeeded() (err error) {
	// Lock using initialization mutex
	this.initializationMutex.Lock()
	defer this.initializationMutex.Unlock()

	// Check if file is already open (thread-safe)
	if this.file != nil {
		return
	}

	// Start measuring the operation time
	startTime := MonoUnixTimeMilliFloat()

	// Open file
	this.file, err = FileDescriptors.OpenAndIncrement(this.filePath, os.O_RDWR, 0666)
	if err != nil {
		return
	}

	// Get file size
	fileSize, err := this.GetFileSize()
	if err != nil {
		this.Release()
		return
	}

	// Create index
	this.index = NewDatastoreIndex()
	err = this.index.AddFromEntryStream(NewPrefetchingReaderAt(this.file), 0, fileSize)

	if err != nil {
		// Check if file ends unexpectedly, or last entry does not include a transaction end marker
		if err == io.ErrUnexpectedEOF {
			this.parentServer.Log(fmt.Sprintf("Possible incomplete transcacion found in datastore '%s'. Attempting roll-back..", this.name), 1)

			// Attempt to roll back to last succesful transaction
			err = this.TryRollingBackToLastSuccessfulTransaction()
			if err != nil {
				this.Release()
				return
			}

			// Get file size again
			fileSize, err = this.GetFileSize()
			if err != nil {
				this.Release()
				return
			}
		} else {

			// Index creation failed for some other reason, release and fail.
			this.Release()
			return
		}
	}

	// Check if file is empty. I.e. it doesn't have a creation entry.
	// Note this could have been caused by a previous truncation to 0 size.
	if fileSize == 0 {
		this.parentServer.Log(fmt.Sprintf("Datastore file '%s' has length 0. Adding creation entry..", this.name), 1)
		// Reset the file with a valid creation entry
		_, err = io.Copy(this.file, CreateNewDatastoreReaderFromBytes([]byte{}, MonoUnixTimeMicro()))

		// If some error occured while trying to reset the file
		if err != nil {
			// Release and return the error
			this.Release()
			return
		}

		// Get file size again
		fileSize, err = this.GetFileSize()

		// If some error occured while trying to get the file size
		if err != nil {
			// Release and return the error
			this.Release()
			return
		}

		// Recreate index
		this.index = NewDatastoreIndex()
		err = this.index.AddFromEntryStream(this.file, 0, fileSize)

		// If some error occured while trying to recreate the index
		if err != nil {
			// Release and return the error
			this.Release()
			return
		}
	}

	// If this is a configuration datastore, cache its content
	if this.IsConfig() { 
		var updatedDataCache *VarMap

		// Load and deserialize the file's content
		updatedDataCache, err = this.GetUpdatedDataCache(this.file, 0, fileSize)

		// If some error occured while trying load the file's content
		if err != nil {
			// Release and return the error
			this.Release()
			return
		}

		// Atomically replace the data cache with the new one
		this.dataCache = updatedDataCache
	} else {
		// Otherwise, load corresponding configuration datastore, if needed
		err = this.configDatastore.LoadIfNeeded()

		// If some error occured while trying load the configuration datastore
		if err != nil {
			switch err.(type) {
			case *os.PathError:
				// If the error was a "not found" error, that's OK, it means there simply 
				// datastore, isn't a configuration datastore for this datastore.
				// set the error to 'nil', and continue
				err = nil
			default:
				// Otherwise, release and return the error
				this.Release()
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
func (this *DatastoreOperationsEntry) CreateReader(updatedAfter int64) (reader io.Reader, readSize int64, err error) {
	// Make sure the datastore is open
	if this.file == nil {
		return nil, 0, DatastoreNotOpenErr
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
func (this *DatastoreOperationsEntry) CommitTransaction(transactionBytes []byte) (commitTimestamp int64, err error) {
	// Make sure the datastore is open
	if this.file == nil {
		return 0, DatastoreNotOpenErr
	}
	
	// If the transaction is empty, return without error
	if len(transactionBytes) == 0 {
		return
	}

	// Get the datastore size limit
	datastoreSizeLimit, _ := this.GetInt64ConfigValue("['datastore']['limit']['maxSize']")

	// Make sure the transaction wouldn't cause it to exceed this limit
	if datastoreSizeLimit > 0 && this.index.TotalSize+int64(len(transactionBytes)) > datastoreSizeLimit {
		return 0, DatastoreTooLargeErr{fmt.Sprintf("Datastore '%s' is limited to a maximum size of %d bytes", this.name, datastoreSizeLimit)}
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
		updatedDataCache, err = this.GetUpdatedDataCache(bytes.NewReader(transactionBytes), 0, int64(len(transactionBytes)))

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

	// If compaction was not needed, schedule a flush, if needed.
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

func (this *DatastoreOperationsEntry) Rewrite(transactionBytes []byte) (commitTimestamp int64, err error) {
	// Note: no need to check if the datastore is open here, this should succeed even if it is closed

	// Get the datastore size limit
	datastoreSizeLimit, _ := this.GetInt64ConfigValue("['datastore']['limit']['maxSize']")

	// Make sure the transaction wouldn't cause it to exceed this limit
	if datastoreSizeLimit > 0 && int64(len(transactionBytes)) > datastoreSizeLimit {
		return 0, DatastoreTooLargeErr{fmt.Sprintf("Datastore '%s' is limited to a maximum size of %d bytes", this.name, datastoreSizeLimit)}
	}

	// Get a safe commit timestamp (must be strictly greater than a previous commit timestamp)
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
		// Get an updated data cache value that would replace the old cache value
		// once all write operations have successfuly completed		
		updatedDataCache, err = this.GetUpdatedDataCache(bytes.NewReader(transactionBytes), 0, int64(len(transactionBytes)))
		if err != nil {
			return
		}
	}

	// Close the file and release all resources
	err = this.Release()
	if err != nil {
		return
	}

	// Reset compaction state file (all of its values would be reset to zero)
	err = this.resetCompactionState()
	if err != nil {
		return
	}

	// Safely replace file the datastore file with a creation entry and the new transaction as content
	err = ReplaceFileSafely(this.filePath, CreateNewDatastoreReaderFromBytes(transactionBytes, commitTimestamp))
	if err != nil {
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

func (this *DatastoreOperationsEntry) ScheduleFlushIfNeeded() {
	// If a flush is already scheduled, return immediately
	if (this.flushScheduled) {
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
	if (maxDelayToFlush == 0) {
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

func (this *DatastoreOperationsEntry) GetUpdatedDataCache(entryStreamReader io.ReaderAt, startOffset int64, endOffset int64) (updatedCache *VarMap, err error) {
	if this.dataCache == nil {
		updatedCache = NewEmptyVarMap()
	} else {
		updatedCache = this.dataCache.Clone()
	}

	err = DeserializeEntryStreamReaderAndAppendToVarMap(entryStreamReader, startOffset, endOffset, updatedCache)

	if err != nil {
		return nil, err
	}

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Compaction operations
///////////////////////////////////////////////////////////////////////////////////////////////////
func (this *DatastoreOperationsEntry) CompactIfNeeded() (bool, error) {
	startTime := MonoUnixTimeMilli()

	currentSize := this.index.TotalSize

	// Read configuration options for compaction
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

	err := this.loadCompactionStateIfNeeded()
	if err != nil {
		return false, err
	}

	// Continue only if file size has grown a sufficient amount since last check
	if float64(currentSize) < float64(this.compactionState.LastCompactionCheckSize)*compactionMinGrowthRatio {
		return false, nil
	}

	// Create a key index and add all entries to it
	keyIndex := NewDatastoreKeyIndex()
	err = keyIndex.AddFromEntryStream(NewPrefetchingReaderAt(this.file), 0, currentSize)
	if err != nil {
		return false, err
	}

	// Get compacted size and calculate unused size
	compactedSize := keyIndex.GetCompactedSize()
	unusedSize := currentSize - compactedSize

	// Continue only if compacted size is above threshold for a file rewrite
	if float64(unusedSize)/float64(currentSize) < compactionMinUnusedSizeRatio {

		// Update compaction state for latest compaction check results
		this.compactionState.LastCompactionCheckTime = MonoUnixTimeMicro()
		this.compactionState.LastCompactionCheckSize = currentSize
		this.compactionState.LastCompactionCheckUnusedSize = unusedSize

		err = this.storeCompactionState()
		if err != nil {
			return false, err
		}

		return false, nil
	} else {
		// Prepare for compaction: reset compaction state file
		this.resetCompactionState()
	}

	// Rewrite the file with the compacted ranges and updated metadata
	compactedDatastoreReader := keyIndex.CreateReaderForCompactedRanges(this.file, 0)

	err = ReplaceFileSafely(this.filePath, compactedDatastoreReader)
	if err != nil {
		return false, err
	}

	// Update compaction state file to the compacted size
	this.compactionState.LastCompactionCheckTime = MonoUnixTimeMicro()
	this.compactionState.LastCompactionCheckSize = compactedSize
	this.compactionState.LastCompactionCheckUnusedSize = 0

	err = this.storeCompactionState()
	if err != nil {
		return false, err
	}

	// Release datastore resources
	err = this.Release()
	if err != nil {
		return false, err
	}

	this.parentServer.Log(fmt.Sprintf("Compacted datastore '%s' from %d to %d bytes in %dms", this.name, currentSize, compactedSize, MonoUnixTimeMilli()-startTime), 1)

	return true, nil
}

func (this *DatastoreOperationsEntry) loadCompactionStateIfNeeded() error {
	if this.compactionState != nil {
		return nil
	} else {
		this.compactionState = &DatastoreCompactionState{}
		fileContent, err := ReadEntireFile(this.compactionStateFilePath())

		if err == nil {
			err = json.Unmarshal(fileContent, this.compactionState)

			if err != nil {
				return err
			}
		}

		// Address the case where the compaction state file is invalid
		if this.compactionState.LastCompactionCheckSize > this.index.TotalSize {
			this.resetCompactionState()
		}

		return nil
	}
}

func (this *DatastoreOperationsEntry) storeCompactionState() (err error) {
	newFileContent, err := json.Marshal(this.compactionState)
	if err != nil {
		return
	}

	err = RewriteFile(this.compactionStateFilePath(), bytes.NewReader(newFileContent), false)

	return
}

func (this *DatastoreOperationsEntry) resetCompactionState() (err error) {
	this.compactionState = &DatastoreCompactionState{}
	err = this.storeCompactionState()
	return
}

func (this *DatastoreOperationsEntry) deleteCompactionStateFile() (err error) {
	this.compactionState = nil
	err = os.Remove(this.compactionStateFilePath())
	return
}

func (this *DatastoreOperationsEntry) compactionStateFilePath() string {
	return this.filePath + ".compactionState"
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Cleanup and destruction operations
///////////////////////////////////////////////////////////////////////////////////////////////////
func (this *DatastoreOperationsEntry) Release() (err error) {
	if this.file == nil {
		return nil
	}

	err = FileDescriptors.Decrement(this.file)
	if err != nil {
		return
	}

	this.file = nil
	this.index = nil
	this.compactionState = nil

	// The cached data shouldn't be cleared here
	// Otherwise configuration would become nil every time the datastore is rewritten
	// or an error occurs:
	//this.dataCache = nil

	return
}

func (this *DatastoreOperationsEntry) Destroy() (err error) {
	err = this.Release()
	if err != nil {
		return
	}

	this.deleteCompactionStateFile()
	err = DeleteFileSafely(this.filePath)
	if err != nil {
		return
	}

	// The cached content is only wiped once the datastore has been successfuly
	// Destroyed
	this.dataCache = nil

	this.updateNotifier.AnnounceUpdate(math.MaxInt64)

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Repair operations
///////////////////////////////////////////////////////////////////////////////////////////////////
func (this *DatastoreOperationsEntry) TryRollingBackToLastSuccessfulTransaction() (err error) {
	fileSize, err := this.GetFileSize()
	if err != nil {
		return
	}

	truncatedSize, err := FindSafeTruncationSize(NewPrefetchingReaderAt(this.file), fileSize)

	if err != nil {
		return
	}

	if truncatedSize == fileSize {
		this.parentServer.Log(fmt.Sprintf("No need to repair datastore '%s'", this.name), 1)
		return
	}

	err = this.file.Truncate(truncatedSize)
	if err == nil {
		this.parentServer.Log(fmt.Sprintf("Truncated datastore '%s' from %d to %d bytes", this.name, fileSize, truncatedSize), 1)

		// Try recreating journal index up to the truncated size
		this.index = NewDatastoreIndex()
		err = this.index.AddFromEntryStream(this.file, 0, truncatedSize)

		if err != nil {
			this.parentServer.Log(fmt.Sprintf("Failed to recreate index for datastore '%s' after repair", this.name), 1)
		}
	} else {
		this.parentServer.Log(fmt.Sprintf("Error while attempting roll-back of datastore '%s'", this.name), 1)
	}

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Configuration lookup operations
///////////////////////////////////////////////////////////////////////////////////////////////////
func (this *DatastoreOperationsEntry) GetStringConfigValue(key string) (value string, err error) {
	if this.configDatastore != nil {
		cachedDatastoreConfig := this.configDatastore.dataCache

		if cachedDatastoreConfig != nil {
			value, err = cachedDatastoreConfig.GetString(key)
			if err == nil {
				return
			}
		}
	}

	return this.parentServer.GlobalConfig().GetString(key)
}

func (this *DatastoreOperationsEntry) GetBoolConfigValue(key string) (value bool, err error) {
	if this.configDatastore != nil {
		cachedDatastoreConfig := this.configDatastore.dataCache

		if cachedDatastoreConfig != nil {
			value, err = cachedDatastoreConfig.GetBool(key)
			if err == nil {
				return
			}
		}
	}

	return this.parentServer.GlobalConfig().GetBool(key)
}

func (this *DatastoreOperationsEntry) GetInt64ConfigValue(key string) (value int64, err error) {
	if this.configDatastore != nil {
		cachedDatastoreConfig := this.configDatastore.dataCache

		if cachedDatastoreConfig != nil {
			value, err = cachedDatastoreConfig.GetInt64(key)
			if err == nil {
				return
			}
		}
	}

	return this.parentServer.GlobalConfig().GetInt64(key)
}

func (this *DatastoreOperationsEntry) GetFloat64ConfigValue(key string) (value float64, err error) {
	if this.configDatastore != nil {
		cachedDatastoreConfig := this.configDatastore.dataCache

		if cachedDatastoreConfig != nil {
			value, err = cachedDatastoreConfig.GetFloat64(key)
			if err == nil {
				return
			}
		}
	}

	return this.parentServer.GlobalConfig().GetFloat64(key)
}

func (this *DatastoreOperationsEntry) GetFileSize() (fileSize int64, err error) {
	if this.file == nil {
		return 0, DatastoreNotOpenErr
	}

	fileInfo, err := this.file.Stat()
	if err != nil {
		return
	}

	fileSize = fileInfo.Size()

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Misc operations
///////////////////////////////////////////////////////////////////////////////////////////////////
func (this *DatastoreOperationsEntry) LastModifiedTime() int64 {
	return this.index.LatestUpdateTimestamp()
}

func (this *DatastoreOperationsEntry) IsConfig() bool {
	return strings.HasSuffix(this.name, ".config")
}

func (this *DatastoreOperationsEntry) IsGlobalConfig() bool {
	return this.name == ".config"
}

func (this *DatastoreOperationsEntry) IsCached() bool {
	return this.IsConfig()
}

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

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Construction and global operations
///////////////////////////////////////////////////////////////////////////////////////////////////
func NewDatastoreOperationsEntry(datastoreName string, parentServer *Server) *DatastoreOperationsEntry {
	return &DatastoreOperationsEntry{
		parentServer: parentServer,

		name:                datastoreName,
		filePath:            parentServer.startupOptions.StoragePath + "/" + datastoreName,
		file:                nil,
		initializationMutex: &sync.Mutex{},
		flushMutex:          &sync.Mutex{},
		index:               nil,
		compactionState:     nil,
		updateNotifier:      NewDatastoreUpdateNotifier(),
		configDatastore:     nil,

		dataCache:   nil,
		rateLimiter: NewRequestRateLimiter(),
	}
}

type DatastoreCompactionState struct {
	LastCompactionCheckTime       int64
	LastCompactionCheckSize       int64
	LastCompactionCheckUnusedSize int64
}

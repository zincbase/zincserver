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
	"time"
	//"log"
)

type DatastoreOperationsEntry struct {
	name string

	filePath            string
	file                *os.File
	initializationMutex *sync.Mutex
	flushMutex          *sync.Mutex
	flushScheduled      bool

	compactionState *DatastoreCompactionState
	index           *DatastoreIndex
	updateNotifier  *DatastoreUpdateNotifier

	parentServer *Server

	dataCache   *VarMap
	rateLimiter *RequestRateLimiter

	configDatastore *DatastoreOperationsEntry

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

			// Attempt roll back to last succesful transaction
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
		_, err = io.Copy(this.file, CreateNewDatastoreReaderFromBytes([]byte{}, MonoUnixTimeMicro()))

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

		// Recreate index
		this.index = NewDatastoreIndex()
		err = this.index.AddFromEntryStream(this.file, 0, fileSize)

		if err != nil {
			this.Release()
			return
		}
	}

	if this.IsConfig() { // If this is a configuration datastore, cache its content
		var updatedDataCache *VarMap

		updatedDataCache, err = this.GetUpdatedDataCache(this.file, 0, fileSize)
		if err != nil {
			this.Release()
			return
		}

		this.dataCache = updatedDataCache
	} else { // Otherwise, load corresponding configuration datastore, if needed
		err = this.configDatastore.LoadIfNeeded()

		if err != nil {
			switch err.(type) {
			case *os.PathError:
				err = nil
			default:
				this.Release()
				return
			}
		}
	}

	this.parentServer.Log(fmt.Sprintf("Loaded datastore '%s' in %fms", this.name, MonoUnixTimeMilliFloat()-startTime), 1)
	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Read operations
///////////////////////////////////////////////////////////////////////////////////////////////////
func (this *DatastoreOperationsEntry) CreateReader(updatedAfter int64) (reader io.Reader, readSize int64, err error) {
	if this.file == nil {
		return nil, 0, DatastoreNotOpenErr
	}

	offset := this.index.FindOffsetOfFirstEntryUpdatedAfter(updatedAfter)
	if offset == -1 {
		return EmptyReader{}, 0, nil
	}

	reader = NewRangeReader(this.file, offset, int64(this.index.TotalSize))
	readSize = this.index.TotalSize - offset

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// Write operations
///////////////////////////////////////////////////////////////////////////////////////////////////
func (this *DatastoreOperationsEntry) CommitTransaction(transactionBytes []byte) (commitTimestamp int64, err error) {
	if this.file == nil {
		return 0, DatastoreNotOpenErr
	}

	if len(transactionBytes) == 0 {
		return
	}

	// Check size limits
	datastoreSizeLimit, _ := this.GetInt64ConfigValue("['datastore']['limit']['maxSize']")

	if datastoreSizeLimit > 0 && this.index.TotalSize+int64(len(transactionBytes)) > datastoreSizeLimit {
		return 0, DatastoreTooLargeErr{fmt.Sprintf("Datastore '%s' is limited to a maximum size of %d bytes", this.name, datastoreSizeLimit)}
	}

	// Get commit timestamp
	commitTimestamp = this.GetColisionFreeTimestamp()

	// Validate and prepare transaction: rewrite timestamp and ensure transaction end mark
	// for last entry
	err = ValidateAndPrepareTransaction(transactionBytes, commitTimestamp)
	if err != nil {
		return
	}

	// If this is a datastore should be cached, get an updated data cache value that
	// would replace the old cache value once all write operations complete successfuly
	var updatedDataCache *VarMap

	if this.IsCached() {
		updatedDataCache, err = this.GetUpdatedDataCache(bytes.NewReader(transactionBytes), 0, int64(len(transactionBytes)))
		if err != nil {
			return
		}
	}

	// Write the transaction to the file
	_, err = this.file.WriteAt(transactionBytes, int64(this.index.TotalSize))
	if err != nil {
		return
	}

	// Update index
	err = this.index.AppendFromBuffer(transactionBytes)
	if err != nil {
		return
	}

	// Perform a compaction check and compact if needed
	compacted, err := this.CompactIfNeeded()
	if err != nil {
		return
	}

	// If compaction did not occur, schedule a flush, if needed.
	if !compacted {
		this.ScheduleFlushIfNeeded()
	}

	// Update cache if needed
	if updatedDataCache != nil {
		this.dataCache = updatedDataCache
	}

	// Announce the update
	this.updateNotifier.AnnounceUpdate(commitTimestamp)
	return
}

func (this *DatastoreOperationsEntry) Rewrite(transactionBytes []byte) (commitTimestamp int64, err error) {
	// Check size limits
	datastoreSizeLimit, _ := this.GetInt64ConfigValue("['datastore']['limit']['maxSize']")

	if datastoreSizeLimit > 0 && int64(len(transactionBytes)) > datastoreSizeLimit {
		return 0, DatastoreTooLargeErr{fmt.Sprintf("Datastore '%s' is limited to a maximum size of %d bytes", this.name, datastoreSizeLimit)}
	}

	// Get commit timestamp
	commitTimestamp = this.GetColisionFreeTimestamp()

	// Validate and prepare transaction: rewrite timestamp and ensure transaction end mark
	// for last entry
	err = ValidateAndPrepareTransaction(transactionBytes, commitTimestamp)
	if err != nil {
		return
	}

	// If this datastore should be cached, get an updated data cache value that
	// would replace the old cache value once all write operations complete successfuly
	var updatedDataCache *VarMap

	if this.IsCached() {
		updatedDataCache, err = this.GetUpdatedDataCache(bytes.NewReader(transactionBytes), 0, int64(len(transactionBytes)))
		if err != nil {
			return
		}
	}

	// Close file and release all resources
	err = this.Release()
	if err != nil {
		return
	}

	// Reset compaction state file
	err = this.resetCompactionState()
	if err != nil {
		return
	}

	// Replace file
	err = ReplaceFileSafely(this.filePath, CreateNewDatastoreReaderFromBytes(transactionBytes, commitTimestamp))
	if err != nil {
		return
	}

	// Update cache if needed
	if updatedDataCache != nil {
		this.dataCache = updatedDataCache
	}

	// Announce the update
	this.updateNotifier.AnnounceUpdate(commitTimestamp)
	return
}

func (this *DatastoreOperationsEntry) ScheduleFlushIfNeeded() {
	flushEnabled, err := this.GetBoolConfigValue("['datastore']['flush']['enabled']")

	if err != nil || flushEnabled == false {
		return
	}

	maxDelayToFlush, err := this.GetInt64ConfigValue("['datastore']['flush']['maxDelay']")

	if err != nil || maxDelayToFlush < 0 || this.flushScheduled {
		return
	}

	this.flushScheduled = true

	targetFile := this.file
	FileDescriptors.Increment(targetFile)

	go func() {
		if maxDelayToFlush > 0 {
			time.Sleep(time.Duration(maxDelayToFlush) * time.Millisecond)
		}

		this.flushMutex.Lock()
		defer this.flushMutex.Unlock()

		this.flushScheduled = false

		startTime := MonoUnixTimeMilli()
		err := targetFile.Sync()
		if err == nil {
			this.parentServer.Log(fmt.Sprintf("Flushed datastore '%s' in %dms", this.name, MonoUnixTimeMilli()-startTime), 1)
		} else {
			this.parentServer.Log(fmt.Sprintf("Error flushing datastore '%s'. %s", this.name, err.Error()), 1)
		}
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
	if this.index == nil {
		return
	}

	lastModifiedTime := this.LastModifiedTime()

	// Spinwait, if needed, until new timestamp is strictly greater than previous one
	for timestamp == lastModifiedTime {
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

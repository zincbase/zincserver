package main

import (
	"os"
	"sync"
	"time"
)

// Flush scheduler object type
type DatastoreFlushScheduler struct {
	flushStartTime time.Duration
	scheduleLock   *sync.Mutex
	closed         bool
}

// Flush scheduler object constructor
func NewDatastoreFlushScheduler() *DatastoreFlushScheduler {
	return &DatastoreFlushScheduler{
		flushStartTime: 0,
		scheduleLock:   &sync.Mutex{},
		closed: false,
	}
}

// Ensure the given file is flushed at or before the given interval
func (this *DatastoreFlushScheduler) EnsureFlush(file *os.File, maxDelay time.Duration) (bool, error) {
	// If the scheduler is closed
	if this.closed {
		// Return without error
		return false, nil
	}

	// Lock for scheduling
	this.scheduleLock.Lock()

	// Get current time
	currentTime := time.Duration(MonoUnixTimeNano())

	// If the a future flush is already scheduled
	if this.flushStartTime > currentTime {
		// Unlock for scheduling
		this.scheduleLock.Unlock()

		// Return
		return false, nil
	}

	// Set the flush start time
	this.flushStartTime = currentTime + maxDelay

	// Unlock for scheduling
	this.scheduleLock.Unlock()

	// Wait until the delay time has passed
	time.Sleep(maxDelay)

	// If the scheduler is closed
	if this.closed {
		// Return without error
		return false, nil
	}

	// Flush the file
	err := file.Sync()

	// If an error occurred
	if err != nil {
		// Return the error
		return false, err
	}

	return true, nil
}

func (this *DatastoreFlushScheduler) FlushScheduled() bool {
	return this.flushStartTime > time.Duration(MonoUnixTimeNano())
}

func (this *DatastoreFlushScheduler) Close() {
	this.closed = true
}

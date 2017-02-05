package main

import (
	"sync"
)

// Rate limiter definition
type RateLimiter struct {
	// A lookup table taking a client id and giving a rate limiter entry
	idLookup map[string]*RateLimiterEntry

	// Make this lockable
	sync.Mutex
}

// Rate limiter lookup entry definition
type RateLimiterEntry struct {
	// Time the last measurement has starter
	StartTime int64
	// Number of events counted
	Count int64
}

// Create a new rate limiter object
func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		idLookup: map[string]*RateLimiterEntry{},
	}
}

// Process an event
func (this *RateLimiter) ProcessEvent(clientID string, operation string, timeInterval int64, limit int64) bool {
	// Lock this object
	this.Lock()

	// Unlock when the function returns
	defer this.Unlock()

	// Concat the client id and operation to get the key in the lookup table
	key := clientID + " " + operation
	// Record the current time
	currentTime := MonoUnixTimeMilli()

	// Look for an existing entry for the given key
	entry, found := this.idLookup[key]

	if !found { // If an existing entry wasn't found
		// Create a new entry with current time
		entry = &RateLimiterEntry{
			StartTime: currentTime,
			Count:     0,
		}

		// Add the new entry
		this.idLookup[key] = entry

		// Otherwise if the start time of the entry has expired relative to the given interval
	} else if currentTime > entry.StartTime+timeInterval {
		// Reset the entry
		entry.Count = 0
		entry.StartTime = currentTime
	}

	// If the event count has reached or passed the limit
	if entry.Count >= limit {
		// Return false
		return false
	} else { // Otherwise
		// Increment the count
		entry.Count += 1
		return true
	}
}

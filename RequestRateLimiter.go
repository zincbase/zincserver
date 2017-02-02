package main

import (
	"sync"
)

type RequestRateLimiter struct {
	entries map[string]*RequestRateLimiterEntry
	sync.RWMutex
}

func NewRequestRateLimiter() *RequestRateLimiter {
	return &RequestRateLimiter{
		entries: map[string]*RequestRateLimiterEntry{},
	}
}

func (this *RequestRateLimiter) ProcessRequest(userID string, operation string, timeInterval int64, limit int64) bool {
	this.Lock()
	defer this.Unlock()

	entryKey := userID + " " + operation
	currentTime := MonoUnixTimeMilli()

	entry, found := this.entries[entryKey]
	if !found {
		entry = &RequestRateLimiterEntry{
			StartTime: currentTime,
			Count:     0,
		}

		this.entries[entryKey] = entry
	} else if currentTime > entry.StartTime+timeInterval {
		entry.Count = 0
		entry.StartTime = currentTime
	}

	if entry.Count >= limit {
		return false
	} else {
		entry.Count += 1
		return true
	}
}

type RequestRateLimiterEntry struct {
	StartTime int64
	Count     int64
}

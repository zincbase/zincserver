package main

import (
	"sync"
)

// The subscriber object
type DatastoreUpdateSubscriber struct {
	waitGroup             *sync.WaitGroup
	minTimestampThreshold int64
}

// The notifier object
type DatastoreUpdateNotifier struct {
	subscribers []*DatastoreUpdateSubscriber
	sync.Mutex
}

func NewDatastoreUpdateNotifier() *DatastoreUpdateNotifier {
	return &DatastoreUpdateNotifier{
		subscribers: []*DatastoreUpdateSubscriber{},
	}
}

// Create a new notification channel for the given timestamp threshold.
// Note this assumes that updates before or at the given timestamp have already been handled
func (this *DatastoreUpdateNotifier) CreateUpdateNotification(minTimestampThreshold int64) (waitGroup *sync.WaitGroup) {
	// Create a wait group
	waitGroup = &sync.WaitGroup{}

	// Increment the wait group
	waitGroup.Add(1)

	// Lock this object
	this.Lock()

	// Add a new subscriber
	this.subscribers = append(this.subscribers, &DatastoreUpdateSubscriber{
		waitGroup:             waitGroup,
		minTimestampThreshold: minTimestampThreshold,
	})

	// Unlock this object
	this.Unlock()

	return
}

// Announce an update has occurred, with the given timestamp as its occurrence time
func (this *DatastoreUpdateNotifier) AnnounceUpdate(timestamp int64) {
	// Lock this object
	this.Lock()

	// Unlock it when the function finishes
	defer this.Unlock()

	// Get current subscriber list
	currentSubscribers := this.subscribers

	// If there are no subscribers
	if len(currentSubscribers) == 0 {
		// Return
		return
	}

	// Replace the subscriber list with a new one
	this.subscribers = []*DatastoreUpdateSubscriber{}

	// For each of the current subscribers
	for _, subscriber := range currentSubscribers {
		// If the subscriber's timestamp threshold is less than the given timestamp
		if subscriber.minTimestampThreshold < timestamp {
			subscriber.waitGroup.Done()
		} else { // Otherwise
			// Add the subscriber to the new subscriber list
			this.subscribers = append(this.subscribers, subscriber)
		}
	}
}

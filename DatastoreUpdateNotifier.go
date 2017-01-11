package main

import (
	"sync"
)

type DatastoreUpdateSubscriber struct {
	channel               chan int64
	minTimestampThreshold int64
}

type DatastoreUpdateNotifier struct {
	subscribers []*DatastoreUpdateSubscriber
	sync.Mutex
}

func (this *DatastoreUpdateNotifier) CreateUpdateNotificationChannel(minTimestampThreshold int64) (resultChannel chan int64) {
	// If there are already changes past the given timestamp, they might need to be notified
	// immediately!

	resultChannel = make(chan int64)

	this.Lock()
	this.subscribers = append(this.subscribers, &DatastoreUpdateSubscriber{
		channel:               resultChannel,
		minTimestampThreshold: minTimestampThreshold,
	})
	this.Unlock()

	return
}

func (this *DatastoreUpdateNotifier) AnnounceUpdate(timestamp int64) {
	this.Lock()
	defer this.Unlock()

	currentSubscribers := this.subscribers
	if len(currentSubscribers) == 0 {
		return
	}

	this.subscribers = []*DatastoreUpdateSubscriber{}

	for _, subscriber := range currentSubscribers {
		if subscriber.minTimestampThreshold < timestamp {
			// Non-blocking channel send
			select {
			case subscriber.channel <- timestamp:
			default:
			}
		} else {
			this.subscribers = append(this.subscribers, subscriber)
		}
	}
}

func NewDatastoreUpdateNotifier() *DatastoreUpdateNotifier {
	return &DatastoreUpdateNotifier{
		subscribers: []*DatastoreUpdateSubscriber{},
	}
}

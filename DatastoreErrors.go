package main

import (
	"errors"
)

var ErrDatastoreNotOpen = errors.New("Datastore is not open")

type ErrDatastoreSizeLimitExceeded struct {
	message string
}

func (this ErrDatastoreSizeLimitExceeded) Error() string {
	return this.message
}

type ErrEntryRejected struct {
	message string
}

func (this ErrEntryRejected) Error() string {
	return this.message
}

var ErrCorruptedEntry = errors.New("Invalid entry checksum detected. This may be due to data corruption.")
var ErrInvalidHeadEntry = errors.New("Invalid head entry detected.")
var ErrEmptyTransaction = errors.New("An empty transaction bytestream was given.")

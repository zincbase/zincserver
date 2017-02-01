package main

import (
	"errors"
)

var ErrDatastoreNotOpen = errors.New("Datastore is not open")

type ErrDatastoreTooLarge struct {
	message string
}

func (this ErrDatastoreTooLarge) Error() string {
	return this.message
}

var ErrCorruptedEntry = errors.New("Invalid entry checksum detected. This may be due to data corruption.")

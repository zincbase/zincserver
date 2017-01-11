package main

import (
	"errors"
)

var DatastoreNotOpenErr = errors.New("Datastore is not open")

type DatastoreTooLargeErr struct {
	message string
}

func (this DatastoreTooLargeErr) Error() string {
	return this.message
}

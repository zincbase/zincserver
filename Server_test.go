package main

import (
	"log"
	//"reflect"
	"testing"
	"time"
	//"bytes"
	//"sourcegraph.com/sqs/goreturns/returns"
)

func Test_Server(t *testing.T) {
	setEntryTimestamps := func(entries []Entry, timestamp int64) {
		for i := 0; i < len(entries); i++ {
			entries[i].PrimaryHeader.CommitTime = timestamp
		}
	}

	getTestEntries := func() []Entry {
		return []Entry{
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte(`"Key1"`), []byte(`"Value1"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte(`"Key2"`), []byte(`"Value2"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte(`"Key3"`), []byte(`"Value3"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte(`"Key4"`), []byte(`"Value4"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte(`"Key5"`), []byte(`"Value5"`)},
		}
	}

	config := DefaultServerStartupOptions()
	config.InsecurePort = 12345
	config.StoragePath = "./tests_temp/"
	config.NoAutoMasterKey = true

	var testEntries []Entry
	var server *Server
	var client *Client

	beforeEach := func() {
		testEntries = getTestEntries()
		server = NewServer(config)
		client = NewClient("http://localhost:12345", RandomWordString(10), "")
		server.Start()
	}

	afterEach := func() {
		client.Delete()
		server.Stop()
		time.Sleep(10 * time.Millisecond)
	}

	t.Run("Put and Get entries", func(t *testing.T) {
		beforeEach()
		defer afterEach()

		commitTimestamp, err := client.Put(testEntries[0:2])
		if err != nil {
			t.Error(err)
		}

		log.Println(commitTimestamp)
		setEntryTimestamps(testEntries[0:2], commitTimestamp)

		returnedEntries, err := client.Get(0)

		if err != nil {
			t.Error(err)
		}

		log.Println(returnedEntries)
		log.Println(testEntries[0:2])

		if !EntryArraysAreEqual(returnedEntries[1:], testEntries[0:2]) {
			t.Error("Returned data did not match")
		}

		if returnedEntries[0].PrimaryHeader.Flags&Flag_CreationEvent != Flag_CreationEvent ||
			len(returnedEntries[0].Key) != 0 ||
			len(returnedEntries[0].Value) != 0 {
			t.Error("Invalid creation entry returned")
		}
	})

	t.Run("Post and Get entries", func(t *testing.T) {
		beforeEach()
		defer afterEach()

		commitTimestamp, err := client.Put(testEntries[0:2])
		if err != nil {
			t.Error(err)
		}

		setEntryTimestamps(testEntries[0:2], commitTimestamp)

		commitTimestamp, err = client.Post(testEntries[2:5])
		if err != nil {
			t.Error(err)
		}

		setEntryTimestamps(testEntries[2:5], commitTimestamp)

		returnedEntries, err := client.Get(0)

		if err != nil {
			t.Error(err)
		}

		log.Println(returnedEntries)

		if !EntryArraysAreEqual(returnedEntries[1:], testEntries[0:5]) {
			t.Error("Returned data did not match")
		}

		if returnedEntries[0].PrimaryHeader.Flags&Flag_CreationEvent != Flag_CreationEvent ||
			len(returnedEntries[0].Key) != 0 ||
			len(returnedEntries[0].Value) != 0 {
			t.Error("Invalid creation entry returned")
		}
	})

	t.Run("Put, Post and Get entries after timestamp", func(t *testing.T) {
		beforeEach()
		defer afterEach()

		commitTimestamp, err := client.Put(testEntries[0:2])
		log.Println(commitTimestamp)
		if err != nil {
			t.Error(err)
		}

		setEntryTimestamps(testEntries[0:2], commitTimestamp)

		commitTimestamp, err = client.Post(testEntries[2:5])
		if err != nil {
			t.Error(err)
		}

		setEntryTimestamps(testEntries[2:5], commitTimestamp)

		returnedEntries, err := client.Get(commitTimestamp - 1)

		if err != nil {
			t.Error(err)
		}

		log.Println(returnedEntries)

		if !EntryArraysAreEqual(returnedEntries, testEntries[2:5]) {
			t.Error("Returned data did not match")
		}
	})
}

package main

import (
	"log"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server", func() {
	setEntryTimestamps := func(entries []Entry, timestamp int64) {
		for i := 0; i < len(entries); i++ {
			entries[i].PrimaryHeader.CommitTime = timestamp
		}
	}

	getTestEntries := func() []Entry {
		return []Entry{
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 1"), []byte(`"Key1"`), []byte(`"Value1"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 2"), []byte(`"Key2"`), []byte(`"Value2"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 3"), []byte(`"Key3"`), []byte(`"Value3"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 4"), []byte(`"Key4"`), []byte(`"Value4"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 5"), []byte(`"Key5"`), []byte(`"Value5"`)},
		}
	}

	config := DefaultServerStartupOptions()
	config.InsecurePort = 12345
	config.StoragePath = "./tests_temp/"
	config.NoAutoMasterKey = true

	var testEntries []Entry
	var server *Server
	var client *Client

	BeforeEach(func() {
		testEntries = getTestEntries()
		server = NewServer(config)
		client = NewClient("http://localhost:12345", RandomWordString(10), "")
		server.Start()
	})

	AfterEach(func() {
		client.Delete()
		server.Stop()
		time.Sleep(10 * time.Millisecond)
	})

	It("Puts and gets entries", func() {
		commitTimestamp, err := client.Put(testEntries[0:2])

		Expect(err).To(BeNil())
		Expect(commitTimestamp).ToNot(BeNil())
		Expect(commitTimestamp).To(BeNumerically(">", 0))

		setEntryTimestamps(testEntries[0:2], commitTimestamp)

		returnedEntries, err := client.Get(0)

		Expect(err).To(BeNil())

		log.Println(returnedEntries)
		log.Println(testEntries[0:2])

		ExpectEntryArraysToBeEqual(returnedEntries[1:], testEntries[0:2])

		Expect(returnedEntries[0].PrimaryHeader.Flags & Flag_CreationEvent).To(Equal(Flag_CreationEvent))
		Expect(returnedEntries[0].Key).To(HaveLen(0))
		Expect(returnedEntries[0].Value).To(HaveLen(0))
	})

	It("Posts and gets entries", func() {
		commitTimestamp, err := client.Put(testEntries[0:2])

		Expect(err).To(BeNil())
		Expect(commitTimestamp).ToNot(BeNil())
		Expect(commitTimestamp).To(BeNumerically(">", 0))

		setEntryTimestamps(testEntries[0:2], commitTimestamp)

		commitTimestamp, err = client.Post(testEntries[2:5])
		Expect(err).To(BeNil())
		Expect(commitTimestamp).ToNot(BeNil())
		Expect(commitTimestamp).To(BeNumerically(">", 0))

		setEntryTimestamps(testEntries[2:5], commitTimestamp)

		returnedEntries, err := client.Get(0)

		Expect(err).To(BeNil())
		ExpectEntryArraysToBeEqual(returnedEntries[1:], testEntries[0:5])

		Expect(returnedEntries[0].PrimaryHeader.Flags & Flag_CreationEvent).To(Equal(Flag_CreationEvent))
		Expect(returnedEntries[0].Key).To(HaveLen(0))
		Expect(returnedEntries[0].Value).To(HaveLen(0))
	})

	It("Puts, posts and gets entries after timestamp", func() {
		commitTimestamp, err := client.Put(testEntries[0:2])

		Expect(err).To(BeNil())
		Expect(commitTimestamp).ToNot(BeNil())
		Expect(commitTimestamp).To(BeNumerically(">", 0))

		setEntryTimestamps(testEntries[0:2], commitTimestamp)

		commitTimestamp, err = client.Post(testEntries[2:5])
		Expect(err).To(BeNil())
		Expect(commitTimestamp).ToNot(BeNil())
		Expect(commitTimestamp).To(BeNumerically(">", 0))

		setEntryTimestamps(testEntries[2:5], commitTimestamp)

		returnedEntries, err := client.Get(commitTimestamp - 1)

		Expect(err).To(BeNil())
		ExpectEntryArraysToBeEqual(returnedEntries, testEntries[2:5])
	})
})

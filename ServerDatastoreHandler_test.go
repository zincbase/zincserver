package main

import (
	//"log"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	//"log"
	"math/rand"
	//"log"
)

var _ = Describe("Server", func() {
	getTestEntries := func() []Entry {
		return []Entry{
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 1"), []byte(`"Key1"`), []byte(`"Value1"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 2"), []byte(`"Key2"`), []byte(`"Value2"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 3"), []byte(`"Key3"`), []byte(`"Value3"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 4"), []byte(`"Key4"`), []byte(`"Value4"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 5"), []byte(`"Key5"`), []byte(`"Value5"`)},
		}
	}

	getRandomEntries := func() []Entry {
		return GenerateRandomEntries(1+rand.Intn(9), 1+rand.Intn(9), rand.Intn(100), "randomBinaryEntry")
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

		returnedEntries, err := client.Get(0)

		Expect(err).To(BeNil())

		//log.Println(returnedEntries)
		//log.Println(testEntries[0:2])

		ExpectEntryArraysToBeEquivalent(returnedEntries[1:], testEntries[0:2])

		Expect(returnedEntries[0].Key).To(HaveLen(0))
		Expect(returnedEntries[0].Value).To(HaveLen(HeadEntryValueSize))
	})

	It("Posts and gets entries", func() {
		commitTimestamp, err := client.Put(testEntries[0:2])

		Expect(err).To(BeNil())
		Expect(commitTimestamp).ToNot(BeNil())
		Expect(commitTimestamp).To(BeNumerically(">", 0))

		commitTimestamp, err = client.Post(testEntries[2:5])
		Expect(err).To(BeNil())
		Expect(commitTimestamp).ToNot(BeNil())
		Expect(commitTimestamp).To(BeNumerically(">", 0))

		returnedEntries, err := client.Get(0)

		Expect(err).To(BeNil())
		ExpectEntryArraysToBeEquivalent(returnedEntries[1:], testEntries[0:5])

		Expect(returnedEntries[0].Key).To(HaveLen(0))
		Expect(returnedEntries[0].Value).To(HaveLen(HeadEntryValueSize))
	})

	It("Puts, posts and gets entries after timestamp", func() {
		commitTimestamp, err := client.Put(testEntries[0:2])

		Expect(err).To(BeNil())
		Expect(commitTimestamp).ToNot(BeNil())
		Expect(commitTimestamp).To(BeNumerically(">", 0))

		commitTimestamp, err = client.Post(testEntries[2:5])
		Expect(err).To(BeNil())
		Expect(commitTimestamp).ToNot(BeNil())
		Expect(commitTimestamp).To(BeNumerically(">", 0))

		returnedEntries, err := client.Get(commitTimestamp - 1)

		Expect(err).To(BeNil())
		ExpectEntryArraysToBeEquivalent(returnedEntries, testEntries[2:5])
	})

	It("Repeatedly posts a random transaction and gets it", func() {
		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())

		for i := 0; i < 20; i++ {
			randomEntries := getRandomEntries()

			commitTimestamp, err := client.Post(randomEntries)

			Expect(err).To(BeNil())
			Expect(commitTimestamp).ToNot(BeNil())
			Expect(commitTimestamp).To(BeNumerically(">", 0))

			returnedEntries, err := client.Get(commitTimestamp - 1)

			Expect(err).To(BeNil())
			ExpectEntryArraysToBeEquivalent(returnedEntries, randomEntries)
			ExpectEntriesToHaveCommitTimestamp(returnedEntries, commitTimestamp)
		}
	})

	It("Repeatedly puts a random transaction and gets it", func() {
		for i := 0; i < 20; i++ {
			randomEntries := getRandomEntries()

			commitTimestamp, err := client.Put(randomEntries)

			Expect(err).To(BeNil())
			Expect(commitTimestamp).ToNot(BeNil())
			Expect(commitTimestamp).To(BeNumerically(">", 0))

			returnedEntries, err := client.Get(commitTimestamp - 1)

			Expect(err).To(BeNil())
			ExpectEntryArraysToBeEquivalent(returnedEntries[1:], randomEntries)
			ExpectEntriesToHaveCommitTimestamp(returnedEntries[1:], commitTimestamp)
		}
	})

	It("Repeatedly puts a random transaction, gets it, and deletes the datastore", func() {
		for i := 0; i < 20; i++ {
			randomEntries := getRandomEntries()

			commitTimestamp, err := client.Put(randomEntries)

			Expect(err).To(BeNil())
			Expect(commitTimestamp).ToNot(BeNil())
			Expect(commitTimestamp).To(BeNumerically(">", 0))

			returnedEntries, err := client.Get(commitTimestamp - 1)

			Expect(err).To(BeNil())
			ExpectEntryArraysToBeEquivalent(returnedEntries[1:], randomEntries)
			ExpectEntriesToHaveCommitTimestamp(returnedEntries[1:], commitTimestamp)

			err = client.Delete()
			Expect(err).To(BeNil())
		}
	})

	It("Repeatedly puts a random transaction, gets it, posts another random transaction, gets it, gets all, and deletes the datastore", func() {
		for i := 0; i < 20; i++ {
			// Send a PUT request for the first transaction

			randomEntries1 := getRandomEntries()

			commitTimestamp1, err := client.Put(randomEntries1)

			Expect(err).To(BeNil())
			Expect(commitTimestamp1).ToNot(BeNil())
			Expect(commitTimestamp1).To(BeNumerically(">", 0))

			// Send a GET request for the first transaction

			returnedEntries1, err := client.Get(commitTimestamp1 - 1)

			Expect(err).To(BeNil())
			ExpectEntryArraysToBeEquivalent(returnedEntries1[1:], randomEntries1)
			ExpectEntriesToHaveCommitTimestamp(returnedEntries1[1:], commitTimestamp1)

			/// Send a POST request for the second transaction

			randomEntries2 := getRandomEntries()

			commitTimestamp2, err := client.Post(randomEntries2)

			Expect(err).To(BeNil())
			Expect(commitTimestamp2).ToNot(BeNil())
			Expect(commitTimestamp2).To(BeNumerically(">", 0))

			// Send a GET request for the second transaction
			returnedEntries2, err := client.Get(commitTimestamp2 - 1)

			Expect(err).To(BeNil())
			ExpectEntryArraysToBeEquivalent(returnedEntries2, randomEntries2)
			ExpectEntriesToHaveCommitTimestamp(returnedEntries2[1:], commitTimestamp2)

			/// Send a GET request for the whole datastore

			returnedEntries3, err := client.Get(0)

			Expect(err).To(BeNil())
			ExpectEntryArraysToBeEquivalent(returnedEntries3[1:], append(randomEntries1, randomEntries2...))
			ExpectEntriesToHaveCommitTimestamp(returnedEntries3[1:1+len(randomEntries1)], commitTimestamp1)
			ExpectEntriesToHaveCommitTimestamp(returnedEntries3[1+len(randomEntries1):], commitTimestamp2)

			//
			err = client.Delete()
			Expect(err).To(BeNil())
		}
	})

	It("Errors on GET requests to non-existing datastores", func() {
		_, err := client.Get(0)
		Expect(err).NotTo(BeNil())
	})

	It("Errors on POST requests to non-existing datastores", func() {
		_, err := client.Post(testEntries)
		Expect(err).NotTo(BeNil())
	})

	It("Errors on POST requests with empty transactions", func() {
		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())
		_, err = client.Post([]Entry{})
		Expect(err).NotTo(BeNil())
	})

	It("Errors on PUT transactions including entries with 0-length keys", func() {
		_, err := client.Put([]Entry{
			Entry{nil, []byte{}, []byte{}, []byte{1, 2, 3}},
		})

		Expect(err).NotTo(BeNil())
	})

	It("Errors on POST transactions including entries with 0-length keys", func() {
		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())

		_, err = client.Post([]Entry{
			Entry{nil, []byte{}, []byte{}, []byte{1, 2, 3}},
		})

		Expect(err).NotTo(BeNil())
	})
})

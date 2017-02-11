package main

import (
	"encoding/hex"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"time"
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

	getRandomEntries := func(maxCount int, maxKeySize int, maxValueSize int) []Entry {
		return GenerateRandomEntries(RandomIntInRange(1, maxCount), maxKeySize, maxValueSize, "randomBinaryEntry")
	}

	config := DefaultServerStartupOptions()
	config.InsecurePort = 12345
	config.StoragePath = "./tests_temp/"
	config.NoAutoMasterKey = true

	const host = "http://localhost:12345"

	putGlobalSetting := func(key string, value string, accessKey string) (err error) {
		globalConfigClient := NewClient(host, ".config", accessKey)

		_, err = globalConfigClient.Post([]Entry{
			Entry{
				PrimaryHeader:        &EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON},
				SecondaryHeaderBytes: nil,
				Key:                  []byte(key),
				Value:                []byte(value),
			},
		})

		return
	}

	putDatastoreSetting := func(datastoreName string, key string, value string, accessKey string) (err error) {
		datastoreConfigClient := NewClient(host, datastoreName+".config", accessKey)

		_, err = datastoreConfigClient.PostOrPut([]Entry{
			Entry{
				PrimaryHeader:        &EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON},
				SecondaryHeaderBytes: nil,
				Key:                  []byte(key),
				Value:                []byte(value),
			},
		})

		return
	}

	destroyDatastoreConfig := func(datastoreName string) {
		datastoreConfigClient := NewClient(host, datastoreName+".config", "")
		datastoreConfigClient.Delete()
	}

	var testEntries []Entry
	var datastoreName string
	var server *Server
	var client *Client

	BeforeEach(func() {
		testEntries = getTestEntries()
		datastoreName = RandomWordString(10)

		UnlinkFileSafe(config.StoragePath + ".config")
		server = NewServer(config)
		server.Start()
		client = NewClient(host, datastoreName, "")
	})

	AfterEach(func() {
		client.Delete()
		destroyDatastoreConfig(datastoreName)
		server.Stop()

		// Ensure KeepAlive http connections are closed
		closer := http.DefaultTransport.(interface {
			CloseIdleConnections()
		})
		closer.CloseIdleConnections()

		UnlinkFileSafe(config.StoragePath + ".config")
	})

	It("Puts and gets entries", func() {
		commitTimestamp, err := client.Put(testEntries[0:2])

		Expect(err).To(BeNil())
		Expect(commitTimestamp).ToNot(BeNil())
		Expect(commitTimestamp).To(BeNumerically(">", 0))

		returnedEntries, err := client.Get(0)

		Expect(err).To(BeNil())

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
			randomEntries := getRandomEntries(10, 10, 50)

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
			randomEntries := getRandomEntries(10, 10, 50)

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
			randomEntries := getRandomEntries(10, 10, 50)

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

			randomEntries1 := getRandomEntries(10, 10, 50)

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

			randomEntries2 := getRandomEntries(10, 10, 50)

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

	It("Executes a series of random operations and matches their results to a simulated datastore state (compaction disabled)", func() {
		settingErr := putDatastoreSetting(client.datastoreName, `"['datastore']['compaction']['enabled']"`, "false", "")
		Expect(settingErr).To(BeNil())

		const maxEntryCount = 10
		const maxKeySize = 40
		const maxValueSize = 5000

		simulator := NewServerDatastoreHandlerSimulator()

		for i := 0; i < 100; i++ {
			operationCode := RandomIntInRange(0, 100)

			if operationCode < 7 { // PUT operation
				randomEntries := getRandomEntries(maxEntryCount, maxKeySize, maxValueSize)

				commitTimestamp, clientErr := client.Put(randomEntries)

				if clientErr != nil {
					simulatorErr := simulator.Put(randomEntries)
					Expect(simulatorErr).NotTo(BeNil())
				} else {
					timestampedEntries, clientErr := client.Get(commitTimestamp - 1)
					Expect(clientErr).To(BeNil())
					ExpectEntryArraysToBeEquivalent(timestampedEntries[1:], randomEntries)

					simulatorErr := simulator.Put(timestampedEntries)
					Expect(simulatorErr).To(BeNil())
				}
			} else if operationCode < 70 { // POST operation
				randomEntries := getRandomEntries(maxEntryCount, maxKeySize, maxValueSize)
				simulator.ReplaceRandomEntriesWithExistingKeyedRandomEntries(randomEntries)

				commitTimestamp, clientErr := client.Post(randomEntries)

				if clientErr != nil {
					simulatorErr := simulator.Post(randomEntries)
					Expect(simulatorErr).NotTo(BeNil())
				} else {
					timestampedEntries, err := client.Get(commitTimestamp - 1)
					Expect(err).To(BeNil())
					ExpectEntryArraysToBeEquivalent(timestampedEntries, randomEntries)

					simulatorErr := simulator.Post(timestampedEntries)
					Expect(simulatorErr).To(BeNil())
				}
			} else if operationCode < 98 { // GET operation
				randomTimestamp := simulator.GetRandomTimestampInCommittedRange()
				clientResult, clientErr := client.Get(randomTimestamp)
				simulatorResult, simulatorErr := simulator.Get(randomTimestamp)

				if clientErr != nil {
					Expect(simulatorErr).NotTo(BeNil())
				} else {
					Expect(simulatorErr).To(BeNil())
					ExpectEntryArraysToBeEqual(clientResult, simulatorResult)
				}
			} else { // DELETE operation
				clientErr := client.Delete()
				simulatorErr := simulator.Delete()

				if clientErr != nil {
					Expect(simulatorErr).NotTo(BeNil())
				} else {
					Expect(simulatorErr).To(BeNil())
				}
			}
		}
	})

	It("Executes a series of random operations and matches their results to a simulated datastore state (compaction enabled)", func() {
		settingErr := putDatastoreSetting(client.datastoreName, `"['datastore']['compaction']['enabled']"`, "true", "")
		Expect(settingErr).To(BeNil())

		settingErr = putDatastoreSetting(client.datastoreName, `"['datastore']['compaction']['minUnusedSizeRatio']"`, "0.1", "")
		Expect(settingErr).To(BeNil())

		const maxEntryCount = 10
		const maxKeySize = 40
		const maxValueSize = 5000

		simulator := NewServerDatastoreHandlerSimulator()

		for i := 0; i < 100; i++ {
			operationCode := RandomIntInRange(0, 100)

			if operationCode < 7 { // PUT operation
				randomEntries := getRandomEntries(maxEntryCount, maxKeySize, maxValueSize)

				commitTimestamp, clientErr := client.Put(randomEntries)

				if clientErr != nil {
					simulatorErr := simulator.Put(randomEntries)
					Expect(simulatorErr).NotTo(BeNil())
				} else {
					timestampedEntries, clientErr := client.Get(commitTimestamp - 1)
					Expect(clientErr).To(BeNil())
					ExpectEntryArraysToBeEquivalentWhenCompacted(timestampedEntries[1:], randomEntries)

					simulatorErr := simulator.Put(timestampedEntries)
					Expect(simulatorErr).To(BeNil())
				}
			} else if operationCode < 70 { // POST operation
				randomEntries := getRandomEntries(maxEntryCount, maxKeySize, maxValueSize)
				simulator.ReplaceRandomEntriesWithExistingKeyedRandomEntries(randomEntries)

				commitTimestamp, clientErr := client.Post(randomEntries)

				if clientErr != nil {
					simulatorErr := simulator.Post(randomEntries)
					Expect(simulatorErr).NotTo(BeNil())
				} else {
					timestampedEntries, err := client.Get(commitTimestamp - 1)
					Expect(err).To(BeNil())
					ExpectEntryArraysToBeEquivalentWhenCompacted(timestampedEntries, randomEntries)

					simulatorErr := simulator.Post(timestampedEntries)
					Expect(simulatorErr).To(BeNil())
				}
			} else if operationCode < 98 { // GET operation
				randomTimestamp := simulator.GetRandomTimestampInCommittedRange()
				clientResult, clientErr := client.Get(randomTimestamp)
				simulatorResult, simulatorErr := simulator.Get(randomTimestamp)

				if clientErr != nil {
					Expect(simulatorErr).NotTo(BeNil())
				} else {
					Expect(simulatorErr).To(BeNil())
					ExpectEntryArraysToBeEquivalentWhenCompacted(clientResult, simulatorResult)
				}
			} else { // DELETE operation
				clientErr := client.Delete()
				simulatorErr := simulator.Delete()

				if clientErr != nil {
					Expect(simulatorErr).NotTo(BeNil())
				} else {
					Expect(simulatorErr).To(BeNil())
				}
			}
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

	It("Rejects requests with invalid master keys", func() {
		// Generate master key
		masterKey := hex.EncodeToString(RandomBytes(16))
		masterKeyHash := SHA1ToHex([]byte(masterKey))
		putGlobalSetting(`"['server']['masterKeyHash']"`, `"`+masterKeyHash+`"`, "")
		defer putGlobalSetting(`"['server']['masterKeyHash']"`, `""`, masterKey)

		// Test client with valid access key
		clientWithValidAccessKey := NewClient(host, client.datastoreName, masterKey)
		_, err := clientWithValidAccessKey.Put(testEntries)
		Expect(err).To(BeNil())
		_, err = clientWithValidAccessKey.Post(testEntries)
		Expect(err).To(BeNil())
		_, err = clientWithValidAccessKey.Get(0)
		Expect(err).To(BeNil())

		// Test client with invalid access key
		invalidMasterKey := hex.EncodeToString(RandomBytes(16))
		clientWithInvalidAccessKey := NewClient(host, client.datastoreName, invalidMasterKey)

		_, err = clientWithInvalidAccessKey.Put(testEntries)
		Expect(err).NotTo(BeNil())
		_, err = clientWithInvalidAccessKey.Post(testEntries)
		Expect(err).NotTo(BeNil())
		_, err = clientWithInvalidAccessKey.Get(0)
		Expect(err).NotTo(BeNil())
	})

	It("Rejects requests with invalid datastore access keys", func() {
		// Generate access key
		accessKey := hex.EncodeToString(RandomBytes(16))
		accessKeyHash := SHA1ToHex([]byte(accessKey))
		settingErr := putDatastoreSetting(client.datastoreName, `"['datastore']['accessKeyHash']['`+accessKeyHash+`']"`, `"ReaderWriter"`, "")
		Expect(settingErr).To(BeNil())

		// Test client with valid access key
		clientWithValidAccessKey := NewClient(host, client.datastoreName, accessKey)
		_, err := clientWithValidAccessKey.Put(testEntries)
		Expect(err).To(BeNil())
		_, err = clientWithValidAccessKey.Post(testEntries)
		Expect(err).To(BeNil())
		_, err = clientWithValidAccessKey.Get(0)
		Expect(err).To(BeNil())

		// Test client with invalid access key
		invalidMasterKey := hex.EncodeToString(RandomBytes(16))
		clientWithInvalidAccessKey := NewClient(host, client.datastoreName, invalidMasterKey)

		_, err = clientWithInvalidAccessKey.Put(testEntries)
		Expect(err).NotTo(BeNil())
		_, err = clientWithInvalidAccessKey.Post(testEntries)
		Expect(err).NotTo(BeNil())
		_, err = clientWithInvalidAccessKey.Get(0)
		Expect(err).NotTo(BeNil())
	})

	It("Rejects requests methods that are not allowed by the profile", func() {
		// Generate access key
		accessKey := hex.EncodeToString(RandomBytes(16))
		accessKeyHash := SHA1ToHex([]byte(accessKey))
		settingErr := putDatastoreSetting(client.datastoreName, `"['datastore']['accessKeyHash']['`+accessKeyHash+`']"`, `"Reader"`, "")
		Expect(settingErr).To(BeNil())

		// Put initial data in the datastore
		client.Put(testEntries)

		// Test client with valid access key
		clientWithValidAccessKey := NewClient(host, client.datastoreName, accessKey)
		_, err := clientWithValidAccessKey.Put(testEntries)
		Expect(err).NotTo(BeNil())
		_, err = clientWithValidAccessKey.Post(testEntries)
		Expect(err).NotTo(BeNil())
		_, err = clientWithValidAccessKey.Get(0)
		Expect(err).To(BeNil())
	})

	It("Rejects requests with parameters that are not allowed by the profile", func() {
		// Generate access key
		accessKey := hex.EncodeToString(RandomBytes(16))
		accessKeyHash := SHA1ToHex([]byte(accessKey))
		settingErr := putDatastoreSetting(client.datastoreName, `"['datastore']['accessKeyHash']['`+accessKeyHash+`']"`, `"Reader"`, "")
		Expect(settingErr).To(BeNil())
		settingErr = putDatastoreSetting(client.datastoreName, `"['accessProfile']['Reader']['method']['GET']['param']['updatedAfter']['allowed']"`, `false`, "")
		Expect(settingErr).To(BeNil())

		// Put initial data in the datastore
		client.Put(testEntries)

		// Test client
		clientForProfile := NewClient(host, client.datastoreName, accessKey)

		_, err := clientForProfile.Get(0)
		Expect(err).To(BeNil())
		_, err = clientForProfile.Get(1)
		Expect(err).NotTo(BeNil())
	})

	It("Enforces rate limits for a particular profile", func() {
		// Generate access key
		accessKey := hex.EncodeToString(RandomBytes(16))
		accessKeyHash := SHA1ToHex([]byte(accessKey))
		settingErr := putDatastoreSetting(client.datastoreName, `"['datastore']['accessKeyHash']['`+accessKeyHash+`']"`, `"Reader"`, "")
		Expect(settingErr).To(BeNil())
		settingErr = putDatastoreSetting(client.datastoreName, `"['accessProfile']['Reader']['method']['GET']['limit']['requests']['interval']"`, `50`, "")
		Expect(settingErr).To(BeNil())
		settingErr = putDatastoreSetting(client.datastoreName, `"['accessProfile']['Reader']['method']['GET']['limit']['requests']['count']"`, `2`, "")
		Expect(settingErr).To(BeNil())


		// Put initial data in the datastore
		client.Put(testEntries)

		// Test client
		clientForProfile := NewClient(host, client.datastoreName, accessKey)

		_, err := clientForProfile.Get(0)
		Expect(err).To(BeNil())

		_, err = clientForProfile.Get(0)
		Expect(err).To(BeNil())

		_, err = clientForProfile.Get(0)
		Expect(err).NotTo(BeNil())
		time.Sleep(50 * time.Millisecond)

		_, err = clientForProfile.Get(0)
		Expect(err).To(BeNil())
	})
})

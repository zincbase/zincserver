package main

import (
	"encoding/hex"
	"net/http"
	"time"

	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/gorilla/websocket"
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

	//////////////////////////////////////////////////////////////////////////////////////////////////////
	/// Operation tests
	//////////////////////////////////////////////////////////////////////////////////////////////////////
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

		for i := 0; i < 10; i++ {
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

		for i := 0; i < 10; i++ {
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

	It("Serves a GET request with waitUntilNonempty enabled", func() {
		commitTimestamp, putErr := client.Put([]Entry{})
		Expect(putErr).To(BeNil())

		var result []Entry
		var err error

		go func() {
			result, err = client.GetWhenNonEmpty(commitTimestamp)
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			_, postErr := client.Post(testEntries)
			Expect(postErr).To(BeNil())
		}()

		Eventually(func() []Entry { return result }).Should(HaveLen(len(testEntries)))
		ExpectEntryArraysToBeEquivalent(result, testEntries)
	})

	It("Serves a WebSocket connection", func() {
		_, putErr := client.Put(testEntries)
		Expect(putErr).To(BeNil())

		nextResult, err := client.OpenWebSocket(0)

		Expect(err).To(BeNil())
		result, err := nextResult()

		Expect(err).To(BeNil())
		ExpectEntryArraysToBeEquivalent(result[1:], testEntries)

		randomEntries := []Entry{*getRandomBinaryEntry(20, 500), *getRandomBinaryEntry(20, 200)}

		go func() {
			result, err = nextResult()
			Log("Result: ", result)
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			_, postErr := client.Post(randomEntries)
			Expect(postErr).To(BeNil())
		}()

		Eventually(func() []Entry { return result }).Should(HaveLen(len(randomEntries)))
		ExpectEntryArraysToBeEquivalent(result, randomEntries)
	})

	It("Compacts the datastore if a certain threshold is reached", func() {
		//
		configErr := putDatastoreSetting(datastoreName, `"['datastore']['compaction']['enabled']"`, "true", "")
		Expect(configErr).To(BeNil())
		configErr = putDatastoreSetting(datastoreName, `"['datastore']['compaction']['minGrowthRatio']"`, "1", "")
		Expect(configErr).To(BeNil())
		configErr = putDatastoreSetting(datastoreName, `"['datastore']['compaction']['minSize']"`, "3000", "")
		Expect(configErr).To(BeNil())
		configErr = putDatastoreSetting(datastoreName, `"['datastore']['compaction']['minUnusedSizeRatio']"`, "0.3", "")
		Expect(configErr).To(BeNil())

		randomEntry := getRandomBinaryEntry(20, 1000)

		// Put the entry and get the entire content
		_, putErr := client.Put([]Entry{*randomEntry})
		Expect(putErr).To(BeNil())

		result, getErr := client.Get(0)
		Expect(getErr).To(BeNil())
		Expect(len(result)).To(Equal(2))

		// Transmit the entry again and get the entire content
		_, putErr = client.Post([]Entry{*randomEntry})
		Expect(putErr).To(BeNil())

		result, getErr = client.Get(0)
		Expect(getErr).To(BeNil())
		Expect(len(result)).To(Equal(3))

		// Transmit the entry again and get the entire content
		_, putErr = client.Post([]Entry{*randomEntry})
		Expect(putErr).To(BeNil())

		result, getErr = client.Get(0)
		Expect(getErr).To(BeNil())
		Expect(len(result)).To(Equal(2))
	})

	//////////////////////////////////////////////////////////////////////////////////////////////////////
	/// Operation error tests
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	It("Rejects requests to invalid datastore names", func() {
		invalidClient := NewClient(host, "", "")
		_, err := invalidClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))

		invalidClient = NewClient(host, RandomWordString(129), "")
		_, err = invalidClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects GET requests to non-existing datastores", func() {
		_, err := client.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("404"))
	})

	It("Rejects POST requests to non-existing datastores", func() {
		_, err := client.Post(testEntries)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("404"))
	})

	It("Rejects POST requests with empty transactions", func() {
		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())
		_, err = client.Post([]Entry{})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects PUT transactions including entries with 0-length keys", func() {
		_, err := client.Put([]Entry{
			Entry{nil, []byte{}, []byte{}, []byte{1, 2, 3}},
		})

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects POST transactions including entries with 0-length keys", func() {
		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())

		_, err = client.Post([]Entry{
			Entry{nil, []byte{}, []byte{}, []byte{1, 2, 3}},
		})

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects invalid PUT entry streams", func() {
		for length := 1; length < 50; length++ {
			for i := 0; i < 10; i++ {
				_, _, err := client.Request("PUT", nil, bytes.NewReader(RandomBytes(length)))
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(ContainSubstring("400"))
			}
		}
	})

	It("Rejects invalid POST entry streams", func() {
		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())

		for length := 1; length < 50; length++ {
			for i := 0; i < 10; i++ {
				_, _, err := client.Request("POST", nil, bytes.NewReader(RandomBytes(i)))
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(ContainSubstring("400"))
			}
		}
	})

	It("Rejects DELETE requests to the global configuaration datastore", func() {
		globalConfigClient := NewClient(host, ".config", "")
		err := globalConfigClient.Delete()
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("405"))
	})

	It("Rejects access keys with invalid lengths", func() {
		invalidKeyClient := NewClient(host, datastoreName, "abcd")
		_, err := invalidKeyClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects access keys with invalid characters", func() {
		invalidAccessKey := hex.EncodeToString(RandomBytes(16))
		invalidAccessKey = invalidAccessKey[:5] + string('X') + invalidAccessKey[6:]
		Expect(len(invalidAccessKey)).To(Equal(32))

		invalidKeyClient := NewClient(host, datastoreName, invalidAccessKey)

		_, err := invalidKeyClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Terminates a websocket if it attempts to send a binary or text message to the server", func() {
		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())

		requestURL := "ws://" + client.BuildRequestURL(nil)[7:]
		dialer := &websocket.Dialer{}
		conn, _, err := dialer.Dial(requestURL, nil)
		Expect(err).To(BeNil())

		_, reader, err := conn.NextReader()
		Expect(err).To(BeNil())
		ReadEntireStream(reader)

		var websocketReaderErr error
		go func() {
			_, _, websocketReaderErr = conn.NextReader()
		}()

		go func() {
			time.Sleep(50 * time.Millisecond)

		}()
		conn.WriteMessage(websocket.BinaryMessage, RandomBytes(1))

		Eventually(func() error { return websocketReaderErr }).ShouldNot(BeNil())
	})

	//////////////////////////////////////////////////////////////////////////////////////////////////////
	/// Server configuration tests
	//////////////////////////////////////////////////////////////////////////////////////////////////////
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
		Expect(err.Error()).To(ContainSubstring("401"))

		_, err = clientWithInvalidAccessKey.Post(testEntries)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("401"))

		_, err = clientWithInvalidAccessKey.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("401"))
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
		Expect(err.Error()).To(ContainSubstring("401"))

		_, err = clientWithInvalidAccessKey.Post(testEntries)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("401"))

		_, err = clientWithInvalidAccessKey.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("401"))
	})

	It("Rejects requests to a configuaration datastore, not using the master key", func() {
		// Generate access key
		accessKey := hex.EncodeToString(RandomBytes(16))
		accessKeyHash := SHA1ToHex([]byte(accessKey))

		// Allow it to access to all datastores
		settingErr := putGlobalSetting(`"['datastore']['accessKeyHash']['`+accessKeyHash+`']"`, `"ReaderWriter"`, "")
		Expect(settingErr).To(BeNil())

		// Try the access key with a target datastore, to verify it works
		clientWithValidAccessKey := NewClient(host, client.datastoreName, accessKey)
		_, err := clientWithValidAccessKey.Put(testEntries)
		Expect(err).To(BeNil())

		// Now try that access key with the global configuration datastore
		globalConfigClient := NewClient(host, ".config", accessKey)
		_, err = globalConfigClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("401"))

		// And with a specific configuration datastore
		datastoreConfigClient := NewClient(host, datastoreName+".config", accessKey)
		_, err = datastoreConfigClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("401"))
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

		_, err := clientWithValidAccessKey.Get(0)
		Expect(err).To(BeNil())

		_, err = clientWithValidAccessKey.Put(testEntries)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("403"))

		_, err = clientWithValidAccessKey.Post(testEntries)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("403"))

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
		Expect(err.Error()).To(ContainSubstring("403"))
	})

	It("Enforces rate limits for a particular profile", func() {
		// Generate access key
		accessKey := hex.EncodeToString(RandomBytes(16))
		accessKeyHash := SHA1ToHex([]byte(accessKey))
		settingErr := putDatastoreSetting(client.datastoreName, `"['datastore']['accessKeyHash']['`+accessKeyHash+`']"`, `"Reader"`, "")
		Expect(settingErr).To(BeNil())
		settingErr = putDatastoreSetting(client.datastoreName, `"['accessProfile']['Reader']['method']['GET']['limit']['requests']['interval']"`, `100`, "")
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
		Expect(err.Error()).To(ContainSubstring("429"))

		time.Sleep(100 * time.Millisecond)

		_, err = clientForProfile.Get(0)
		Expect(err).To(BeNil())
	})

	It("Enforces maximum datastore size limits", func() {
		// Set a maximum datastore size
		settingErr := putDatastoreSetting(client.datastoreName, `"['datastore']['limit']['maxSize']"`, `3000`, "")
		Expect(settingErr).To(BeNil())

		_, err := client.Put([]Entry{*getRandomBinaryEntry(20, 3000)})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("403"))

		_, err = client.Put([]Entry{*getRandomBinaryEntry(20, 1000)})
		Expect(err).To(BeNil())

		_, err = client.Post([]Entry{*getRandomBinaryEntry(20, 1000)})
		Expect(err).To(BeNil())

		_, err = client.Post([]Entry{*getRandomBinaryEntry(20, 1000)})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("403"))

		// Set a larger maximum datastore size
		settingErr = putDatastoreSetting(client.datastoreName, `"['datastore']['limit']['maxSize']"`, `4000`, "")
		Expect(settingErr).To(BeNil())

		_, err = client.Post([]Entry{*getRandomBinaryEntry(20, 1000)})
		Expect(err).To(BeNil())
	})

	It("Enforces maximum entry size limits", func() {
		// Set a maximum entry size for this datastore
		settingErr := putDatastoreSetting(client.datastoreName, `"['datastore']['limit']['maxEntrySize']"`, `1000`, "")
		Expect(settingErr).To(BeNil())

		_, err := client.Put([]Entry{*getRandomBinaryEntry(20, 500)})
		Expect(err).To(BeNil())

		_, err = client.Post([]Entry{*getRandomBinaryEntry(20, 500)})
		Expect(err).To(BeNil())

		_, err = client.Put([]Entry{*getRandomBinaryEntry(20, 1500)})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("403"))

		_, err = client.Post([]Entry{*getRandomBinaryEntry(20, 1500)})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("403"))

		// Set a larger maximum entry size for this datastore
		settingErr = putDatastoreSetting(client.datastoreName, `"['datastore']['limit']['maxEntrySize']"`, `2000`, "")
		Expect(settingErr).To(BeNil())

		_, err = client.Post([]Entry{*getRandomBinaryEntry(20, 1500)})
		Expect(err).To(BeNil())

		_, err = client.Put([]Entry{*getRandomBinaryEntry(20, 1500)})
		Expect(err).To(BeNil())
	})
})

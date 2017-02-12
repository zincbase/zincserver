package main

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server operations", func() {
	var context *ServerTestContext

	BeforeEach(func() {
		context = NewServerTestContext()
		context.Start()
	})

	AfterEach(func() {
		context.Stop()
	})

	It("Puts and gets entries", func() {
		client := context.GetClientForRandomDatastore("")
		testEntries := context.GetTestEntries()

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
		client := context.GetClientForRandomDatastore("")
		testEntries := context.GetTestEntries()

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
		client := context.GetClientForRandomDatastore("")
		testEntries := context.GetTestEntries()

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
		client := context.GetClientForRandomDatastore("")

		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())

		for i := 0; i < 20; i++ {
			randomEntries := context.GetRandomEntries(10, 10, 50)

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
		client := context.GetClientForRandomDatastore("")

		for i := 0; i < 20; i++ {
			randomEntries := context.GetRandomEntries(10, 10, 50)

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
		client := context.GetClientForRandomDatastore("")

		for i := 0; i < 20; i++ {
			randomEntries := context.GetRandomEntries(10, 10, 50)

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
		client := context.GetClientForRandomDatastore("")

		for i := 0; i < 20; i++ {
			// Send a PUT request for the first transaction

			randomEntries1 := context.GetRandomEntries(10, 10, 50)

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

			randomEntries2 := context.GetRandomEntries(10, 10, 50)

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

	var testRandomOperations = func(context *ServerTestContext, client *Client, iterations int, compareCompacted bool) {
		var expectEntryArraysToBeEquivalent = func(entries1 []Entry, entries2 []Entry) {
			if compareCompacted {
				ExpectEntryArraysToBeEquivalentWhenCompacted(entries1, entries2)
			} else {
				ExpectEntryArraysToBeEquivalent(entries1, entries2)
			}
		}

		var expectEntryArraysToBeEqual = func(entries1 []Entry, entries2 []Entry) {
			if compareCompacted {
				ExpectEntryArraysToBeEquivalentWhenCompacted(entries1, entries2)
			} else {
				ExpectEntryArraysToBeEqual(entries1, entries2)
			}
		}

		const maxEntryCount = 10
		const maxKeySize = 40
		const maxValueSize = 5000

		simulator := NewServerDatastoreHandlerSimulator()

		for i := 0; i < iterations; i++ {
			operationCode := RandomIntInRange(0, 100)

			if operationCode < 7 { // PUT operation
				randomEntries := context.GetRandomEntries(maxEntryCount, maxKeySize, maxValueSize)

				commitTimestamp, clientErr := client.Put(randomEntries)

				if clientErr != nil {
					simulatorErr := simulator.Put(randomEntries)
					Expect(simulatorErr).NotTo(BeNil())
				} else {
					timestampedEntries, clientErr := client.Get(commitTimestamp - 1)
					Expect(clientErr).To(BeNil())

					expectEntryArraysToBeEquivalent(timestampedEntries[1:], randomEntries)

					simulatorErr := simulator.Put(timestampedEntries)
					Expect(simulatorErr).To(BeNil())
				}
			} else if operationCode < 70 { // POST operation
				randomEntries := context.GetRandomEntries(maxEntryCount, maxKeySize, maxValueSize)
				simulator.ReplaceRandomEntriesWithExistingKeyedRandomEntries(randomEntries)

				commitTimestamp, clientErr := client.Post(randomEntries)

				if clientErr != nil {
					simulatorErr := simulator.Post(randomEntries)
					Expect(simulatorErr).NotTo(BeNil())
				} else {
					timestampedEntries, err := client.Get(commitTimestamp - 1)
					Expect(err).To(BeNil())
					expectEntryArraysToBeEquivalent(timestampedEntries, randomEntries)

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
					expectEntryArraysToBeEqual(clientResult, simulatorResult)
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
	}

	It("Executes a series of random operations and matches their results to a simulated datastore state (single datastore, compaction disabled)", func() {
		client := context.GetClientForRandomDatastore("")

		settingErr := context.PutDatastoreSetting(client.datastoreName, `"['datastore']['compaction']['enabled']"`, "false", "")
		Expect(settingErr).To(BeNil())

		testRandomOperations(context, client, 100, false)
	})

	It("Executes a series of random operations and matches their results to a simulated datastore state (single datastore, compaction enabled)", func() {
		client := context.GetClientForRandomDatastore("")

		settingErr := context.PutDatastoreSettings(client.datastoreName, map[string]string {
			`"['datastore']['compaction']['enabled']"`: "true",
			`"['datastore']['compaction']['minUnusedSizeRatio']"`: "0.1",
		}, "")
		Expect(settingErr).To(BeNil())

		testRandomOperations(context, client, 100, true)
	})

	It("Executes a series of random operations and matches their results to a simulated datastore state (multiple parallel datastores, compaction disabled)", func() {
		client1 := context.GetClientForRandomDatastore("")
		client2 := context.GetClientForRandomDatastore("")

		settingErr := context.PutDatastoreSetting(client1.datastoreName, `"['datastore']['compaction']['enabled']"`, "false", "")
		Expect(settingErr).To(BeNil())

		settingErr = context.PutDatastoreSetting(client2.datastoreName, `"['datastore']['compaction']['enabled']"`, "false", "")
		Expect(settingErr).To(BeNil())
		numberOfTestsFinished := 0

		go func() {
			testRandomOperations(context, client1, 100, false)
			numberOfTestsFinished++;
		}()

		go func() {
			testRandomOperations(context, client2, 100, false)
			numberOfTestsFinished++;
		}()

		Eventually(func() int {return numberOfTestsFinished}, "10s").Should(Equal(2))
	})

	It("Serves a GET request with waitUntilNonempty enabled", func() {
		client := context.GetClientForRandomDatastore("")
		testEntries := context.GetTestEntries()

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
		client := context.GetClientForRandomDatastore("")
		testEntries := context.GetTestEntries()

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
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			_, postErr := client.Post(randomEntries)
			Expect(postErr).To(BeNil())
		}()

		Eventually(func() []Entry { return result }).Should(HaveLen(len(randomEntries)))
		ExpectEntryArraysToBeEquivalent(result, randomEntries)
	})

	It("Compacts the datastore if a certain size threshold is reached", func() {
		client := context.GetClientForRandomDatastore("")

		//
		configErr := context.PutDatastoreSettings(client.datastoreName, map[string]string{
			`"['datastore']['compaction']['enabled']"`:            "true",
			`"['datastore']['compaction']['minGrowthRatio']"`:     "1",
			`"['datastore']['compaction']['minSize']"`:            "3000",
			`"['datastore']['compaction']['minUnusedSizeRatio']"`: "0.3",
		}, "")

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
})

package main

import (
	"encoding/hex"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server", func() {
	var context *ServerTestContext

	BeforeEach(func() {
		context = NewServerTestContext()
		context.Start()
	})

	AfterEach(func() {
		context.Stop()
	})

	//////////////////////////////////////////////////////////////////////////////////////////////////////
	/// Server configuration tests
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	It("Rejects requests with invalid master keys", func() {
		datastoreName := RandomWordString(12)
		testEntries := context.GetTestEntries()

		// Generate master key
		masterKey, masterKeyHash := context.GetRandomAccessKey()

		// Set the generated key as the new master key
		context.PutGlobalSetting(`"['server']['masterKeyHash']"`, `"`+masterKeyHash+`"`, "")
		defer context.PutGlobalSetting(`"['server']['masterKeyHash']"`, `""`, masterKey)

		// Test client with valid access key
		clientWithValidAccessKey := NewClient(context.hostURL, datastoreName, masterKey)
		_, err := clientWithValidAccessKey.Put(testEntries)
		Expect(err).To(BeNil())
		_, err = clientWithValidAccessKey.Post(testEntries)
		Expect(err).To(BeNil())
		_, err = clientWithValidAccessKey.Get(0)
		Expect(err).To(BeNil())

		// Test client with invalid access key
		invalidMasterKey := hex.EncodeToString(RandomBytes(16))
		clientWithInvalidAccessKey := NewClient(context.hostURL, datastoreName, invalidMasterKey)

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
		datastoreName := RandomWordString(12)
		testEntries := context.GetTestEntries()

		// Generate access key
		accessKey, accessKeyHash := context.GetRandomAccessKey()

		// Set the generated access key as an access key for the datastore
		settingErr := context.PutDatastoreSetting(datastoreName, `"['datastore']['accessKeyHash']['`+accessKeyHash+`']"`, `"ReaderWriter"`, "")
		Expect(settingErr).To(BeNil())

		// Test client with valid access key
		clientWithValidAccessKey := NewClient(context.hostURL, datastoreName, accessKey)
		_, err := clientWithValidAccessKey.Put(testEntries)
		Expect(err).To(BeNil())
		_, err = clientWithValidAccessKey.Post(testEntries)
		Expect(err).To(BeNil())
		_, err = clientWithValidAccessKey.Get(0)
		Expect(err).To(BeNil())

		// Test client with invalid access key
		invalidMasterKey := hex.EncodeToString(RandomBytes(16))
		clientWithInvalidAccessKey := NewClient(context.hostURL, datastoreName, invalidMasterKey)

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
		datastoreName := RandomWordString(12)
		testEntries := context.GetTestEntries()

		// Generate access key
		accessKey, accessKeyHash := context.GetRandomAccessKey()

		// Allow it to access to all datastores
		settingErr := context.PutGlobalSetting(`"['datastore']['accessKeyHash']['`+accessKeyHash+`']"`, `"ReaderWriter"`, "")
		Expect(settingErr).To(BeNil())

		// Try the access key with a target datastore, to verify it works
		clientWithValidAccessKey := NewClient(context.hostURL, datastoreName, accessKey)
		_, err := clientWithValidAccessKey.Put(testEntries)
		Expect(err).To(BeNil())

		// Now try that access key with the global configuration datastore
		globalConfigClient := NewClient(context.hostURL, ".config", accessKey)
		_, err = globalConfigClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("401"))

		// And with a specific configuration datastore
		datastoreConfigClient := NewClient(context.hostURL, datastoreName+".config", accessKey)
		_, err = datastoreConfigClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("401"))
	})

	It("Rejects requests methods that are not allowed by the profile", func() {
		client := context.GetClientForRandomDatastore("")
		testEntries := context.GetTestEntries()

		// Generate access key
		accessKey, accessKeyHash := context.GetRandomAccessKey()

		settingErr := context.PutDatastoreSetting(client.datastoreName, `"['datastore']['accessKeyHash']['`+accessKeyHash+`']"`, `"Reader"`, "")
		Expect(settingErr).To(BeNil())

		// Put initial data in the datastore
		client.Put(testEntries)

		// Test client with valid access key
		clientWithValidAccessKey := NewClient(context.hostURL, client.datastoreName, accessKey)

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
		client := context.GetClientForRandomDatastore("")
		testEntries := context.GetTestEntries()

		// Generate access key
		accessKey, accessKeyHash := context.GetRandomAccessKey()

		// Set the generated key as an access key for the datastore
		settingErr := context.PutDatastoreSettings(client.datastoreName, map[string]string{
			`"['datastore']['accessKeyHash']['` + accessKeyHash + `']"`:                          `"Reader"`,
			`"['accessProfile']['Reader']['method']['GET']['param']['updatedAfter']['allowed']"`: `false`,
		}, "")
		Expect(settingErr).To(BeNil())

		// Put initial data in the datastore
		client.Put(testEntries)

		// Test client
		clientForProfile := NewClient(context.hostURL, client.datastoreName, accessKey)

		_, err := clientForProfile.Get(0)
		Expect(err).To(BeNil())
		_, err = clientForProfile.Get(1)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("403"))
	})

	It("Enforces rate limits for a particular profile", func() {
		client := context.GetClientForRandomDatastore("")
		testEntries := context.GetTestEntries()

		// Generate access key
		accessKey, accessKeyHash := context.GetRandomAccessKey()

		// Set config options
		settingErr := context.PutDatastoreSettings(client.datastoreName, map[string]string{
			`"['datastore']['accessKeyHash']['` + accessKeyHash + `']"`:                       `"Reader"`,
			`"['accessProfile']['Reader']['method']['GET']['limit']['requests']['interval']"`: `50`,
			`"['accessProfile']['Reader']['method']['GET']['limit']['requests']['count']"`:    `2`,
		}, "")

		Expect(settingErr).To(BeNil())

		// Put initial data in the datastore
		client.Put(testEntries)

		// Test client
		clientForProfile := NewClient(context.hostURL, client.datastoreName, accessKey)

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
		client := context.GetClientForRandomDatastore("")

		// Set a maximum datastore size
		settingErr := context.PutDatastoreSetting(client.datastoreName, `"['datastore']['limit']['maxSize']"`, `3000`, "")
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
		settingErr = context.PutDatastoreSetting(client.datastoreName, `"['datastore']['limit']['maxSize']"`, `4000`, "")
		Expect(settingErr).To(BeNil())

		_, err = client.Post([]Entry{*getRandomBinaryEntry(20, 1000)})
		Expect(err).To(BeNil())
	})

	It("Enforces maximum entry size limits", func() {
		client := context.GetClientForRandomDatastore("")

		// Set a maximum entry size for this datastore
		settingErr := context.PutDatastoreSetting(client.datastoreName, `"['datastore']['limit']['maxEntrySize']"`, `1000`, "")
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
		settingErr = context.PutDatastoreSetting(client.datastoreName, `"['datastore']['limit']['maxEntrySize']"`, `2000`, "")
		Expect(settingErr).To(BeNil())

		_, err = client.Post([]Entry{*getRandomBinaryEntry(20, 1500)})
		Expect(err).To(BeNil())

		_, err = client.Put([]Entry{*getRandomBinaryEntry(20, 1500)})
		Expect(err).To(BeNil())
	})
})

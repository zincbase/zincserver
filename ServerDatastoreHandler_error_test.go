package main

import (
	"encoding/hex"
	"time"

	"bytes"

	"github.com/gorilla/websocket"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server error handling", func() {
	var context *ServerTestContext

	BeforeEach(func() {
		context = NewServerTestContext()
		context.Start()
	})

	AfterEach(func() {
		context.Stop()
	})

	//////////////////////////////////////////////////////////////////////////////////////////////////////
	/// Operation error tests
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	It("Rejects requests to invalid datastore names", func() {
		invalidClient := NewClient(context.hostURL, "", "")
		_, err := invalidClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))

		invalidClient = NewClient(context.hostURL, RandomWordString(129), "")
		_, err = invalidClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects GET requests to non-existing datastores", func() {
		client := context.GetClientForRandomDatastore("")

		_, err := client.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("404"))
	})

	It("Rejects POST requests to non-existing datastores", func() {
		client := context.GetClientForRandomDatastore("")
		testEntries := context.GetTestEntries()
		_, err := client.Post(testEntries)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("404"))
	})

	It("Rejects POST requests with empty transactions", func() {
		client := context.GetClientForRandomDatastore("")
		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())
		_, err = client.Post([]Entry{})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects PUT transactions including entries with 0-length keys", func() {
		client := context.GetClientForRandomDatastore("")
		_, err := client.Put([]Entry{
			Entry{nil, []byte{}, []byte{}, []byte{1, 2, 3}},
		})

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects POST transactions including entries with 0-length keys", func() {
		client := context.GetClientForRandomDatastore("")

		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())

		_, err = client.Post([]Entry{
			Entry{nil, []byte{}, []byte{}, []byte{1, 2, 3}},
		})

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects PUT and POST transactions including entries with invalid update timestamps", func() {
		client := context.GetClientForRandomDatastore("")

		var getEntryWithUpdateTime = func(updateTime int64) Entry {
			return Entry{&EntryPrimaryHeader{UpdateTime: updateTime}, []byte{}, []byte("Hello"), []byte{1, 2, 3}}
		}

		_, err := client.Put([]Entry{})
		Expect(err).To(BeNil())

		// Try an entry containing current time as update time and verify that works
		_, err = client.Put([]Entry{getEntryWithUpdateTime(MonoUnixTimeMicro())})
		Expect(err).To(BeNil())

		_, err = client.Post([]Entry{getEntryWithUpdateTime(MonoUnixTimeMicro())})
		Expect(err).To(BeNil())

		// Try an entry containing 1 as update time and verify it fails
		_, err = client.Put([]Entry{getEntryWithUpdateTime(1)})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))

		_, err = client.Post([]Entry{getEntryWithUpdateTime(1)})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))

		// Try an entry containing currentTime + 2 minutes as update time and verify it fails
		_, err = client.Put([]Entry{getEntryWithUpdateTime(MonoUnixTimeMicro() + (2 * 60 * 1000 * 1000))})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))

		_, err = client.Post([]Entry{getEntryWithUpdateTime(MonoUnixTimeMicro() + (2 * 60 * 1000 * 1000))})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))

		// Try an entry containing January 1 2017 00:00 (unix time 1483221600 * 1000000) as update time and verify it works
		_, err = client.Put([]Entry{getEntryWithUpdateTime(1483221600 * 1000 * 1000)})
		Expect(err).To(BeNil())

		_, err = client.Post([]Entry{getEntryWithUpdateTime(1483221600 * 1000 * 1000)})
		Expect(err).To(BeNil())

		// Post an entry containing one second before Janurary 1 2017 00:00 as update time and verify it fails
		_, err = client.Put([]Entry{getEntryWithUpdateTime((1483221600 - 1) * 1000 * 1000)})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))

		_, err = client.Post([]Entry{getEntryWithUpdateTime((1483221600 - 1) * 1000 * 1000)})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects invalid PUT entry streams", func() {
		client := context.GetClientForRandomDatastore("")
		for length := 1; length < 50; length++ {
			for i := 0; i < 10; i++ {
				_, _, err := client.Request("PUT", nil, bytes.NewReader(RandomBytes(length)))
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(ContainSubstring("400"))
			}
		}
	})

	It("Rejects invalid POST entry streams", func() {
		client := context.GetClientForRandomDatastore("")
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
		globalConfigClient := NewClient(context.hostURL, ".config", "")
		err := globalConfigClient.Delete()
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("405"))
	})

	It("Rejects access keys with invalid lengths", func() {
		invalidKeyClient := NewClient(context.hostURL, RandomWordString(12), "abcd")
		_, err := invalidKeyClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Rejects access keys with invalid characters", func() {
		invalidAccessKey := hex.EncodeToString(RandomBytes(16))
		invalidAccessKey = invalidAccessKey[:5] + string('X') + invalidAccessKey[6:]
		Expect(len(invalidAccessKey)).To(Equal(32))

		invalidKeyClient := NewClient(context.hostURL, RandomWordString(12), invalidAccessKey)

		_, err := invalidKeyClient.Get(0)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("400"))
	})

	It("Terminates a websocket if it attempts to send a binary or text message to the server", func() {
		client := context.GetClientForRandomDatastore("")

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
})

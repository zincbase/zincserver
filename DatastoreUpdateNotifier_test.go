package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("DatastoreUpdateNotifier", func() {
	It("Announces an update to a single subscriber", func() {
		notifier := NewDatastoreUpdateNotifier()
		waitGroup := notifier.CreateUpdateNotification(51)

		startTime := MonoUnixTimeMilliFloat()
		go func() {
			time.Sleep(10)
			notifier.AnnounceUpdate(52)
		}()
		waitGroup.Wait()
		Expect(MonoUnixTimeMilliFloat() - startTime >= 10)
	})
})

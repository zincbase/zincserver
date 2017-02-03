package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DatastoreUpdateNotifier", func() {
	It("Announces an update to a single subscriber", func() {
		notifier := NewDatastoreUpdateNotifier()
		channel := notifier.CreateUpdateNotificationChannel(51)
		notifier.AnnounceUpdate(52)
		Expect(channel).To(Receive())
	})

	It("Announces an update to multiple subscribers", func() {
		notifier := NewDatastoreUpdateNotifier()

		channel1 := notifier.CreateUpdateNotificationChannel(51)
		channel2 := notifier.CreateUpdateNotificationChannel(34)
		channel3 := notifier.CreateUpdateNotificationChannel(44)
		channel4 := notifier.CreateUpdateNotificationChannel(52)
		channel5 := notifier.CreateUpdateNotificationChannel(53)

		notifier.AnnounceUpdate(52)
		Expect(channel1).To(Receive())
		Expect(channel2).To(Receive())
		Expect(channel3).To(Receive())
		Expect(channel4).NotTo(Receive())
		Expect(channel5).NotTo(Receive())
	})
})

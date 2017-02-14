package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RateLimiter", func() {
	It("Tracks rate limits for an operation", func() {
		rateLimiter := NewRateLimiter()

		Expect(rateLimiter.ProcessEvent("SomeDatastore", "SomeUser", "Open Door", 1000, 2)).To(BeTrue())
		Expect(rateLimiter.ProcessEvent("SomeDatastore", "SomeUser", "Close Door", 1000, 2)).To(BeTrue())

		Expect(rateLimiter.ProcessEvent("SomeDatastore", "SomeUser", "Open Door", 1000, 2)).To(BeTrue())
		Expect(rateLimiter.ProcessEvent("SomeDatastore", "SomeUser", "Open Door", 1000, 2)).To(BeFalse())

		Expect(rateLimiter.ProcessEvent("SomeDatastore", "SomeUser", "Close Door", 1000, 2)).To(BeTrue())
		Expect(rateLimiter.ProcessEvent("SomeDatastore", "SomeUser", "Close Door", 1000, 2)).To(BeFalse())
	})

	It("Expires limit after the given interval passes", func() {
		rateLimiter := NewRateLimiter()

		Expect(rateLimiter.ProcessEvent("SomeDatastore", "SomeUser", "Open Door", 50, 2)).To(BeTrue())
		Expect(rateLimiter.ProcessEvent("SomeDatastore", "SomeUser", "Open Door", 50, 2)).To(BeTrue())
		Expect(rateLimiter.ProcessEvent("SomeDatastore", "SomeUser", "Open Door", 50, 2)).To(BeFalse())

		Eventually(func() bool { return rateLimiter.ProcessEvent("SomeDatastore", "SomeUser", "Open Door", 50, 2) }).Should(BeTrue())
	})
})

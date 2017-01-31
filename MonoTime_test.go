package main

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MonoTime", func() {
	It("Returns a time within a one second range of the system time", func() {
		systemTimeUnixNano := time.Now().UnixNano()
		const oneMinuteInNanoseconds = 1 * 1000000000

		Expect(MonoUnixTimeNano()).To(BeNumerically(">", systemTimeUnixNano-oneMinuteInNanoseconds))
		Expect(MonoUnixTimeNano()).To(BeNumerically("<", systemTimeUnixNano+oneMinuteInNanoseconds))

		systemTimeUnixMicro := systemTimeUnixNano / 1000
		const oneMinuteInMicroseconds = oneMinuteInNanoseconds / 1000
		Expect(MonoUnixTimeMicro()).To(BeNumerically(">", systemTimeUnixMicro-oneMinuteInMicroseconds))
		Expect(MonoUnixTimeMicro()).To(BeNumerically("<", systemTimeUnixMicro+oneMinuteInMicroseconds))

		systemTimeUnixMilli := systemTimeUnixMicro / 1000
		const oneMinuteInMilliseconds = oneMinuteInMicroseconds / 1000
		Expect(MonoUnixTimeMilli()).To(BeNumerically(">", systemTimeUnixMilli-oneMinuteInMilliseconds))
		Expect(MonoUnixTimeMilli()).To(BeNumerically("<", systemTimeUnixMilli+oneMinuteInMilliseconds))

		Expect(MonoUnixTimeMilliFloat()).To(BeNumerically(">", systemTimeUnixMilli-oneMinuteInMilliseconds))
		Expect(MonoUnixTimeMilliFloat()).To(BeNumerically("<", systemTimeUnixMilli+oneMinuteInMilliseconds))
	})
})

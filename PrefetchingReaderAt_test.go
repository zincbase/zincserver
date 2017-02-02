package main

import (
	"bytes"

	"math/rand"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	//"log"
)

var _ = Describe("PrefetchingReaderAt", func() {
	It("Reads from a short slice of bytes", func() {
		randomBytes := RandomBytes(100)

		reader := NewPrefetchingReaderAt(bytes.NewReader(randomBytes))

		buf := make([]byte, 0)
		n, err := reader.ReadAt(buf, 33)

		Expect(err).To(BeNil())
		Expect(n).To(Equal(0))
		Expect(buf).To(Equal([]byte{}))

		buf = make([]byte, 1)
		n, err = reader.ReadAt(buf, 93)

		Expect(err).To(BeNil())
		Expect(n).To(Equal(1))
		Expect(buf).To(Equal(randomBytes[93:94]))

		buf = make([]byte, 3)
		n, err = reader.ReadAt(buf, 11)

		Expect(err).To(BeNil())
		Expect(n).To(Equal(3))
		Expect(buf).To(Equal(randomBytes[11:14]))

		buf = make([]byte, 13)
		n, err = reader.ReadAt(buf, 95)

		Expect(err).To(BeNil())
		Expect(n).To(Equal(5))
		Expect(buf[0:n]).To(Equal(randomBytes[95:]))
	})

	It("Reads from a long (> 32768 characters) slice of bytes", func() {
		randomBytes := RandomBytes(100000)
		reader := NewPrefetchingReaderAt(bytes.NewReader(randomBytes))

		//
		buf := make([]byte, 1)
		n, err := reader.ReadAt(buf, 33333)

		Expect(err).To(BeNil())
		Expect(n).To(Equal(1))
		Expect(buf).To(Equal(randomBytes[33333:33334]))

		//
		buf = make([]byte, 50000)
		n, err = reader.ReadAt(buf, 20000)

		Expect(err).To(BeNil())
		Expect(n).To(Equal(50000))
		Expect(buf).To(Equal(randomBytes[20000 : 20000+50000]))

		//
		buf = make([]byte, 100000-2)
		n, err = reader.ReadAt(buf, 2)

		Expect(err).To(BeNil())
		Expect(n).To(Equal(100000 - 2))
		Expect(buf).To(Equal(randomBytes[2:100000]))

		//
		buf = make([]byte, 10000)
		n, err = reader.ReadAt(buf, 20000)

		Expect(err).To(BeNil())
		Expect(n).To(Equal(10000))
		Expect(buf).To(Equal(randomBytes[20000 : 20000+10000]))
	})

	It("Reads random slices of random lengths from a long (> 32768 characters) slice of bytes", func() {
		randomBytes := RandomBytes(100000)
		reader := NewPrefetchingReaderAt(bytes.NewReader(randomBytes))

		random := rand.New(rand.NewSource(0))

		for i := 0; i < 100; i++ {
			readOffset := random.Intn(100000)
			readLength := random.Intn(100000 - readOffset)

			//log.Printf("Read offset: %d, Read length: %d", readOffset, readLength)

			buf := make([]byte, readLength)
			n, err := reader.ReadAt(buf, int64(readOffset))

			Expect(err).To(BeNil())
			Expect(n).To(Equal(readLength))
			Expect(buf).To(Equal(randomBytes[readOffset : readOffset+readLength]))
		}
	})
})

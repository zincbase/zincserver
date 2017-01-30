package main

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RangeReader", func() {
	It("Reads an entire range", func() {
		data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}

		rangeReader := NewRangeReader(bytes.NewReader(data), 5, 11)
		result := make([]byte, 6)
		rangeReader.Read(result)

		Expect(result).To(Equal(data[5:11]))
	})

	It("Reads a range incrementally ", func() {
		data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}

		rangeReader := NewRangeReader(bytes.NewReader(data), 3, 9)

		result := make([]byte, 1)
		n, err := rangeReader.Read(result)
		Expect(err).To(BeNil())
		Expect(n).To(Equal(1))
		Expect(result).To(Equal(data[3:4]))

		result = make([]byte, 2)
		n, err = rangeReader.Read(result)
		Expect(err).To(BeNil())
		Expect(n).To(Equal(2))
		Expect(result).To(Equal(data[4:6]))

		result = make([]byte, 3)
		n, err = rangeReader.Read(result)
		Expect(err).To(BeNil())
		Expect(n).To(Equal(3))
		Expect(result).To(Equal(data[6:9]))
	})

	It("Reads a range when the given buffer is larger than the remaining size in the range", func() {
		data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}

		rangeReader := NewRangeReader(bytes.NewReader(data), 6, 13)

		result := make([]byte, 20)
		n, err := rangeReader.Read(result)
		Expect(err).To(BeNil())
		Expect(n).To(Equal(7))
		Expect(result[0:7]).To(Equal(data[6:13]))

		result = make([]byte, 5)
		n, err = rangeReader.Read(result)
		Expect(err).NotTo(BeNil())
		Expect(n).To(Equal(0))
	})
})

package main

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RangeListReader", func() {
	It("Reads a list of ranges", func() {
		data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}

		rangeListReader := NewRangeListReader(bytes.NewReader(data), []Range{Range{1, 4}, Range{7, 8}, Range{12, 16}})

		buf := make([]byte, 2)
		n, err := rangeListReader.Read(buf)

		Expect(err).To(BeNil())
		Expect(n).To(Equal(2))
		Expect(buf).To(Equal([]byte{1, 2}))

		buf = make([]byte, 5)
		n, err = rangeListReader.Read(buf)

		Expect(err).To(BeNil())
		Expect(n).To(Equal(5))
		Expect(buf).To(Equal([]byte{3, 7, 12, 13, 14}))
	})
})

package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"bytes"
)

var _ = Describe("SliceUtilities", func() {
	It("Clones a slice", func() {
		Expect(CloneSlice([]byte{1, 2, 3, 4})).To(Equal([]byte{1, 2, 3, 4}))
	})

	It("Clones an empty slice", func() {
		Expect(CloneSlice([]byte{})).To(Equal([]byte{}))
	})

	It("Reads an entire stream to a byte slice", func() {
		testBytes := RandomBytes(100000)
		result, err := ReadEntireStream(bytes.NewReader(testBytes))
		Expect(err).To(BeNil())
		Expect(result).To(Equal(testBytes))
	})
})

package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MemoryWriter", func() {
	It("Appends data to a memory stream", func() {
		memoryWriter := NewMemoryWriter()
		Expect(memoryWriter.WrittenData()).To(Equal([]byte{}))

		memoryWriter.Write([]byte{1, 2, 3})
		Expect(memoryWriter.WrittenData()).To(Equal([]byte{1, 2, 3}))

		memoryWriter.Write([]byte{4, 5})
		Expect(memoryWriter.WrittenData()).To(Equal([]byte{1, 2, 3, 4, 5}))

		memoryWriter.Write([]byte{})
		Expect(memoryWriter.WrittenData()).To(Equal([]byte{1, 2, 3, 4, 5}))

		memoryWriter.Write([]byte{6, 7, 8, 9})
		Expect(memoryWriter.WrittenData()).To(Equal([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}))
	})
})

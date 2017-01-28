package main

import (
    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("BufferUtilities", func() {
	It("Concats a list of buffers", func() {
		buf1 := []byte{1, 2, 3, 4}
		buf2 := []byte{5, 6, 7}
		buf3 := []byte{8, 9}

		Expect(ConcatBufferList([][]byte{buf1, buf2, buf3})).To(
			Equal([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}))
	})

	It("Concats an empty buffer", func() {
		buf1 := []byte{}
		buf2 := []byte{5, 6, 7}
		buf3 := []byte{}

		Expect(ConcatBufferList([][]byte{buf1, buf2, buf3})).To(
			Equal([]byte{5, 6, 7}))
	})

	It("Clones a buffer", func() {
		Expect(CloneBuffer([]byte{1, 2, 3, 4})).To(Equal([]byte{1, 2, 3, 4}))
	})	

	It("Clones an empty buffer", func() {
		Expect(CloneBuffer([]byte{})).To(Equal([]byte{}))
	})	
})

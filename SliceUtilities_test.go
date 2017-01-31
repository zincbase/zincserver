package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SliceUtilities", func() {
	It("Clones a slice", func() {
		Expect(CloneSlice([]byte{1, 2, 3, 4})).To(Equal([]byte{1, 2, 3, 4}))
	})

	It("Clones an empty slice", func() {
		Expect(CloneSlice([]byte{})).To(Equal([]byte{}))
	})
})

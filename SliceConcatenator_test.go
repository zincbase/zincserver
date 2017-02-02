package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SliceConcatenator", func() {
	It("Incrementally concatenates slices", func() {
		concatenator := NewSliceConcatenator()

		Expect(concatenator.Concat()).To(Equal([]byte{}))
		Expect(concatenator.TotalLength).To(EqualNumber(0))

		concatenator.Append([]byte{1, 2, 3, 4})
		Expect(concatenator.TotalLength).To(EqualNumber(4))
		Expect(concatenator.Concat()).To(Equal([]byte{1, 2, 3, 4}))

		concatenator.Append([]byte{5, 6, 7})
		Expect(concatenator.TotalLength).To(EqualNumber(7))
		Expect(concatenator.Concat()).To(Equal([]byte{1, 2, 3, 4, 5, 6, 7}))

		concatenator.Append([]byte{8, 9})
		Expect(concatenator.TotalLength).To(EqualNumber(9))
		Expect(concatenator.Concat()).To(Equal([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}))
	})

	It("Concats a list of slices", func() {
		slice1 := []byte{1, 2, 3, 4}
		slice2 := []byte{5, 6, 7}
		slice3 := []byte{8, 9}

		Expect(ConcatSliceList([][]byte{slice1, slice2, slice3})).To(
			Equal([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}))
	})

	It("Concats empty and nonempty slices", func() {
		slice1 := []byte{}
		slice2 := []byte{5, 6, 7}
		slice3 := []byte{}

		Expect(ConcatSliceList([][]byte{slice1, slice2, slice3})).To(
			Equal([]byte{5, 6, 7}))
	})
})

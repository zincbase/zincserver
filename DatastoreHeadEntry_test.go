package main

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DatastoreHeadEntry", func() {
	It("Creates a serialized head entry", func() {
		serializedHeadEntry := CreateSerializedHeadEntry(&HeadEntryValue{
			Version:                       1234,
			LastCompactionTime:            111111,
			LastCompactionCheckTime:       222222,
			LastCompactionCheckSize:       333333,
			LastCompactionCheckUnusedSize: 444444,
		}, 654321)

		Expect(len(serializedHeadEntry)).To(Equal(HeadEntrySize))

		next := NewEntryStreamIterator(bytes.NewReader(serializedHeadEntry), 0, HeadEntrySize)
		iteratorResult, err := next()

		Expect(err).To(BeNil())
		Expect(iteratorResult.VerifyAllChecksums()).To(BeNil())
		Expect(iteratorResult.Size).To(EqualNumber(HeadEntrySize))
		Expect(iteratorResult.KeySize()).To(EqualNumber(0))
		Expect(iteratorResult.ValueSize()).To(EqualNumber(HeadEntryValueSize))
		Expect(iteratorResult.HasTransactionEndFlag()).To(BeTrue())
		Expect(iteratorResult.PrimaryHeader.CommitTime).To(EqualNumber(654321))
		Expect(iteratorResult.PrimaryHeader.UpdateTime).To(EqualNumber(654321))

		value, err := iteratorResult.ReadValue()

		Expect(err).To(BeNil())

		deserializedValue := DeserializeHeadEntryValue(value)
		Expect(deserializedValue.Version).To(EqualNumber(1234))
		Expect(deserializedValue.LastCompactionTime).To(EqualNumber(111111))
		Expect(deserializedValue.LastCompactionCheckTime).To(EqualNumber(222222))
		Expect(deserializedValue.LastCompactionCheckSize).To(EqualNumber(333333))
		Expect(deserializedValue.LastCompactionCheckUnusedSize).To(EqualNumber(444444))
	})
})

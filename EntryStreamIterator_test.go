package main

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EntryStreamIterator", func() {
	It("Iterates over a stream containing several entries ", func() {
		testEntries := []*Entry{
			&Entry{&EntryPrimaryHeader{CommitTime: 2}, []byte("Secondary header 1"), []byte("Key1"), []byte("Value1")},
			&Entry{&EntryPrimaryHeader{CommitTime: 3}, []byte("Secondary header 2"), []byte("Key2"), []byte("Value2")},
			&Entry{&EntryPrimaryHeader{CommitTime: 3}, []byte("Secondary header 3"), []byte("Key1"), []byte("Value3")},
			&Entry{&EntryPrimaryHeader{CommitTime: 7}, []byte("Secondary header 4"), []byte("Key1"), []byte("Value4")},
			&Entry{&EntryPrimaryHeader{CommitTime: 7}, []byte("Secondary header 5"), []byte("Key2"), []byte("Value5")},
			&Entry{&EntryPrimaryHeader{CommitTime: 13, Flags: Flag_TransactionEnd}, []byte("Secondary header 6"), []byte("Key3"), []byte("Value6")},
		}

		serializedEntries := make([][]byte, len(testEntries))
		for i, entry := range testEntries {
			serializedEntries[i] = SerializeEntry(entry)
		}

		entryStream := ConcatSliceList(serializedEntries)

		moveNext := NewEntryStreamIterator(bytes.NewReader(entryStream), 0, int64(len(entryStream)))

		for i, entry := range testEntries {
			iteratorResult, err := moveNext()
			Expect(err).To(BeNil())

			Expect(iteratorResult.KeySize()).To(EqualNumber(len(entry.Key)))

			key, err := iteratorResult.ReadKey()
			Expect(err).To(BeNil())
			Expect(key).To(Equal(entry.Key))

			keyReader := iteratorResult.CreateKeyReader()
			keyFromReader := make([]byte, len(key))
			keyReader.Read(keyFromReader)
			Expect(err).To(BeNil())
			Expect(keyFromReader).To(Equal(entry.Key))

			Expect(iteratorResult.ValueSize()).To(EqualNumber(len(entry.Value)))

			value, err := iteratorResult.ReadValue()
			Expect(err).To(BeNil())
			Expect(value).To(Equal(entry.Value))

			valueReader := iteratorResult.CreateValueReader()
			valueFromReader := make([]byte, len(value))
			valueReader.Read(valueFromReader)
			Expect(err).To(BeNil())
			Expect(valueFromReader).To(Equal(entry.Value))

			key, value, err = iteratorResult.ReadKeyAndValue()
			Expect(err).To(BeNil())
			Expect(key).To(Equal(entry.Key))
			Expect(value).To(Equal(entry.Value))

			Expect(iteratorResult.SecondaryHeaderSize()).To(EqualNumber(len(entry.SecondaryHeaderBytes)))

			secondaryHeaderBytes, err := iteratorResult.ReadSecondaryHeaderBytes()
			Expect(err).To(BeNil())
			Expect(secondaryHeaderBytes).To(Equal(entry.SecondaryHeaderBytes))

			if i == len(testEntries)-1 {
				Expect(iteratorResult.HasTransactionEndFlag()).To(BeTrue())
			}
		}
	})
})

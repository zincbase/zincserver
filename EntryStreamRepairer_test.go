package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"bytes"
)

var _ = Describe("EntryStreamRepairer", func() {
	It("Returns a truncated size for an incomplete entry stream", func() {
		testEntries := []*Entry{
			&Entry{&EntryPrimaryHeader{CommitTime: 2, Flags: Flag_CreationEvent | Flag_TransactionEnd, }, []byte("Secondary header 1"), []byte("Key1"), []byte("Value1")},
			&Entry{&EntryPrimaryHeader{CommitTime: 3}, []byte("Secondary header 2"), []byte("Key2"), []byte("Value2")},
			&Entry{&EntryPrimaryHeader{CommitTime: 3, Flags: Flag_TransactionEnd}, []byte("Secondary header 3"), []byte("Key1"), []byte("Value3")},
			&Entry{&EntryPrimaryHeader{CommitTime: 7}, []byte("Secondary header 4"), []byte("Key1"), []byte("Value4")},
			&Entry{&EntryPrimaryHeader{CommitTime: 7}, []byte("Secondary header 5"), []byte("Key2"), []byte("Value5")},
			&Entry{&EntryPrimaryHeader{CommitTime: 13, Flags: Flag_TransactionEnd}, []byte("Secondary header 6"), []byte("Key3"), []byte("Value6")},
		}

		serializedEntries := make([][]byte, len(testEntries))
		for i, entry := range testEntries {
			serializedEntries[i] = SerializeEntry(entry)
		}

		cumulativeLengths := make([]int64, len(serializedEntries))
		cumulativeLength := 0
		for i, serializedEntry := range serializedEntries {
			cumulativeLength += len(serializedEntry)
			cumulativeLengths[i] = int64(cumulativeLength)
		}

		entryStream := ConcatBufferList(serializedEntries)

		testTruncationSize := func(endOffset int64, expectedTruncationSize int64) {
			safeTruncationSize1, err := FindSafeTruncationSize(bytes.NewReader(entryStream), endOffset)

			Expect(err).To(BeNil())
			Expect(safeTruncationSize1).To(Equal(expectedTruncationSize))

			safeTruncationSize2, err := FindSafeTruncationSize(bytes.NewReader(entryStream[0:endOffset]), endOffset)

			Expect(err).To(BeNil())
			Expect(safeTruncationSize2).To(Equal(expectedTruncationSize))			
		}

		testTruncationSize(0, 0)
		testTruncationSize(cumulativeLengths[0] - 1, 0)
		testTruncationSize(cumulativeLengths[0], cumulativeLengths[0])
		testTruncationSize(cumulativeLengths[1], cumulativeLengths[0])
		testTruncationSize(cumulativeLengths[2] - 1, cumulativeLengths[0])
		testTruncationSize(cumulativeLengths[2], cumulativeLengths[2])
		testTruncationSize(cumulativeLengths[3], cumulativeLengths[2])
		testTruncationSize(cumulativeLengths[4], cumulativeLengths[2])
		testTruncationSize(cumulativeLengths[5] - 1, cumulativeLengths[2])
		testTruncationSize(cumulativeLengths[5], cumulativeLengths[5])
	})
})
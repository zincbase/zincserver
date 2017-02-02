package main

import (
	"bytes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"math/rand"
)

var _ = Describe("EntryStreamRepairer", func() {
	It("Returns a truncated size for an incomplete datastore stream, for a datastore that was never compacted", func() {
		testEntries := []*Entry{
			CreateHeadEntry(&HeadEntryValue{}, 1234),
			&Entry{&EntryPrimaryHeader{CommitTime: 2, Flags: Flag_TransactionEnd}, []byte("Secondary header 1"), []byte("Key1"), []byte("Value1")},
			&Entry{&EntryPrimaryHeader{CommitTime: 3}, []byte("Secondary header 2"), []byte("Key2"), []byte("Value2")},
			&Entry{&EntryPrimaryHeader{CommitTime: 3, Flags: Flag_TransactionEnd}, []byte("Secondary header 3"), []byte("Key1"), []byte("Value3")},
			&Entry{&EntryPrimaryHeader{CommitTime: 7}, []byte("Secondary header 4"), []byte("Key1"), []byte("Value4")},
			&Entry{&EntryPrimaryHeader{CommitTime: 7}, []byte("Secondary header 5"), []byte("Key2"), []byte("Value5")},
			&Entry{&EntryPrimaryHeader{CommitTime: 13, Flags: Flag_TransactionEnd}, []byte("Secondary header 6"), []byte("Key3"), []byte("Value6")},
		}

		serializedEntries := make([][]byte, len(testEntries))
		cumulativeLengths := make([]int64, len(serializedEntries))
		cumulativeLength := 0

		for i, entry := range testEntries {
			serializedEntries[i] = SerializeEntry(entry)
			AddChecksumsToSerializedEntry(serializedEntries[i])

			cumulativeLength += len(serializedEntries[i])
			cumulativeLengths[i] = int64(cumulativeLength)
		}

		entryStream := ConcatSliceList(serializedEntries)

		testTruncationSize := func(endOffset int64, expectedTruncationSize int64) {
			safeTruncationSize1, err := FindSafeTruncationSize(bytes.NewReader(entryStream), endOffset)

			Expect(err).To(BeNil())
			Expect(safeTruncationSize1).To(Equal(expectedTruncationSize))

			safeTruncationSize2, err := FindSafeTruncationSize(bytes.NewReader(entryStream[0:endOffset]), endOffset)

			Expect(err).To(BeNil())
			Expect(safeTruncationSize2).To(Equal(expectedTruncationSize))
		}

		testTruncationSize(0, 0)
		testTruncationSize(1, 0)
		testTruncationSize(cumulativeLengths[0]-1, 0)
		testTruncationSize(cumulativeLengths[0], cumulativeLengths[0])
		testTruncationSize(cumulativeLengths[1]-1, cumulativeLengths[0])
		testTruncationSize(cumulativeLengths[1], cumulativeLengths[1])
		testTruncationSize(cumulativeLengths[2]-1, cumulativeLengths[1])
		testTruncationSize(cumulativeLengths[2], cumulativeLengths[1])
		testTruncationSize(cumulativeLengths[3]-1, cumulativeLengths[1])
		testTruncationSize(cumulativeLengths[3], cumulativeLengths[3])
		testTruncationSize(cumulativeLengths[4], cumulativeLengths[3])
		testTruncationSize(cumulativeLengths[5], cumulativeLengths[3])
		testTruncationSize(cumulativeLengths[6]-1, cumulativeLengths[3])
		testTruncationSize(cumulativeLengths[6], cumulativeLengths[6])
	})

	It("Returns a truncated size for an incomplete datastore stream, for a datastore that was compacted before", func() {
		testEntries := []*Entry{
			CreateHeadEntry(&HeadEntryValue{ LastCompactionTime: 6 }, 1234),
			&Entry{&EntryPrimaryHeader{CommitTime: 2, Flags: Flag_TransactionEnd}, []byte("Secondary header 1"), []byte("Key1"), []byte("Value1")},
			&Entry{&EntryPrimaryHeader{CommitTime: 3}, []byte("Secondary header 2"), []byte("Key2"), []byte("Value2")},
			&Entry{&EntryPrimaryHeader{CommitTime: 3, Flags: Flag_TransactionEnd}, []byte("Secondary header 3"), []byte("Key1"), []byte("Value3")},
			&Entry{&EntryPrimaryHeader{CommitTime: 7}, []byte("Secondary header 4"), []byte("Key1"), []byte("Value4")},
			&Entry{&EntryPrimaryHeader{CommitTime: 7}, []byte("Secondary header 5"), []byte("Key2"), []byte("Value5")},
			&Entry{&EntryPrimaryHeader{CommitTime: 13, Flags: Flag_TransactionEnd}, []byte("Secondary header 6"), []byte("Key3"), []byte("Value6")},
		}

		serializedEntries := make([][]byte, len(testEntries))
		cumulativeLengths := make([]int64, len(serializedEntries))
		cumulativeLength := 0

		for i, entry := range testEntries {
			serializedEntries[i] = SerializeEntry(entry)
			AddChecksumsToSerializedEntry(serializedEntries[i])

			cumulativeLength += len(serializedEntries[i])
			cumulativeLengths[i] = int64(cumulativeLength)
		}

		entryStream := ConcatSliceList(serializedEntries)

		testTruncationSize := func(endOffset int64, expectedTruncationSize int64) {
			safeTruncationSize1, err := FindSafeTruncationSize(bytes.NewReader(entryStream), endOffset)

			Expect(err).To(BeNil())
			Expect(safeTruncationSize1).To(Equal(expectedTruncationSize))

			safeTruncationSize2, err := FindSafeTruncationSize(bytes.NewReader(entryStream[0:endOffset]), endOffset)

			Expect(err).To(BeNil())
			Expect(safeTruncationSize2).To(Equal(expectedTruncationSize))
		}

		testTruncationSize(0, 0)
		testTruncationSize(1, 0)
		testTruncationSize(cumulativeLengths[0]-1, 0)
		testTruncationSize(cumulativeLengths[0], cumulativeLengths[0])
		testTruncationSize(cumulativeLengths[1]-1, cumulativeLengths[0])
		testTruncationSize(cumulativeLengths[1], cumulativeLengths[1])
		testTruncationSize(cumulativeLengths[2]-1, cumulativeLengths[1])
		testTruncationSize(cumulativeLengths[2], cumulativeLengths[2])
		testTruncationSize(cumulativeLengths[3]-1, cumulativeLengths[2])
		testTruncationSize(cumulativeLengths[3], cumulativeLengths[3])
		testTruncationSize(cumulativeLengths[4], cumulativeLengths[3])
		testTruncationSize(cumulativeLengths[5], cumulativeLengths[3])
		testTruncationSize(cumulativeLengths[6]-1, cumulativeLengths[3])
		testTruncationSize(cumulativeLengths[6], cumulativeLengths[6])
	})	

	It("Returns a correct truncated size for a randomly corrupted entry stream", func() {
		testEntries := []*Entry{
			CreateHeadEntry(&HeadEntryValue{}, 1234),
			&Entry{&EntryPrimaryHeader{CommitTime: 2, Flags: Flag_TransactionEnd}, []byte("Secondary header 1"), []byte("Key1"), []byte("Value1")},
			&Entry{&EntryPrimaryHeader{CommitTime: 3, Flags: Flag_TransactionEnd}, []byte("Secondary header 2"), []byte("Key2"), []byte("Value2")},
			&Entry{&EntryPrimaryHeader{CommitTime: 3, Flags: Flag_TransactionEnd}, []byte("Secondary header 3"), []byte("Key1"), []byte("Value3")},
			&Entry{&EntryPrimaryHeader{CommitTime: 7, Flags: Flag_TransactionEnd}, []byte("Secondary header 4"), []byte("Key1"), []byte("Value4")},
			&Entry{&EntryPrimaryHeader{CommitTime: 7, Flags: Flag_TransactionEnd}, []byte("Secondary header 5"), []byte("Key2"), []byte("Value5")},
			&Entry{&EntryPrimaryHeader{CommitTime: 13, Flags: Flag_TransactionEnd}, []byte("Secondary header 6"), []byte("Key3"), []byte("Value6")},
		}

		serializedEntries := make([][]byte, len(testEntries))
		cumulativeLengths := make([]int, len(serializedEntries))
		cumulativeLength := 0

		for i, entry := range testEntries {
			serializedEntries[i] = SerializeEntry(entry)
			AddChecksumsToSerializedEntry(serializedEntries[i])

			cumulativeLength += len(serializedEntries[i])
			cumulativeLengths[i] = cumulativeLength
		}

		entryStream := ConcatSliceList(serializedEntries)
		random := rand.New(rand.NewSource(1234))

		for i := 0; i < len(entryStream); i++ {
			corruptedEntryStream := CloneSlice(entryStream)
			corruptedCharacterPosition := random.Intn(len(corruptedEntryStream))
			corruptedEntryStream[corruptedCharacterPosition] += byte(1 + random.Intn(100))

			safeTruncationSize, err := FindSafeTruncationSize(bytes.NewReader(corruptedEntryStream), int64(len(corruptedEntryStream)))
			Expect(err).To(BeNil())
			Expect(safeTruncationSize).To(BeNumerically("<", len(corruptedEntryStream)))

			expectedTruncationSize := 0

			for _, cumulativeLength := range cumulativeLengths {
				if cumulativeLength > corruptedCharacterPosition {
					break
				}

				expectedTruncationSize = cumulativeLength
			}

			Expect(safeTruncationSize).To(EqualNumber(expectedTruncationSize))
		}
	})
})

package main

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DatastoreIndex", func() {
	It("Indexes a set of entries", func() {
		index := NewDatastoreIndex()

		testEntries := []*Entry{
			&Entry{&EntryHeader{CommitTime: 1}, []byte("Key1"), []byte("Value1")},
			&Entry{&EntryHeader{CommitTime: 3}, []byte("Key2"), []byte("Value2")},
			&Entry{&EntryHeader{CommitTime: 4}, []byte("Key1"), []byte("Value3")},
			&Entry{&EntryHeader{CommitTime: 7}, []byte("Key1"), []byte("Value4")},
			&Entry{&EntryHeader{CommitTime: 8}, []byte("Key2"), []byte("Value5")},
			&Entry{&EntryHeader{CommitTime: 9}, []byte("Key3"), []byte("Value6")},
			&Entry{&EntryHeader{CommitTime: 13, Flags: Flag_TransactionEnd}, []byte("Key3"), []byte("Value7")},
		}

		serializedEntries := make([][]byte, len(testEntries))
		for i, entry := range testEntries {
			serializedEntries[i] = SerializeEntry(entry)
		}

		testData := ConcatSliceList(serializedEntries)

		err := index.AppendFromEntryStream(bytes.NewReader(testData), 0, int64(len(testData)), nil)
		Expect(err).To(BeNil())

		Expect(int(index.TotalSize)).To(Equal(len(testData)))

		for i, indexEntry := range index.Entries {
			Expect(indexEntry.timestamp).To(Equal(testEntries[i].Header.CommitTime))
		}

		for i, indexEntry := range index.Entries {
			Expect(int(indexEntry.offset)).To(Equal(len(ConcatSliceList(serializedEntries[0:i]))))
		}
	})

	It("Gives the offset of the first entry updated after a given time", func() {
		index := NewDatastoreIndex()

		testEntries := []*Entry{
			&Entry{&EntryHeader{CommitTime: 2}, []byte("Key1"), []byte("Value1")},
			&Entry{&EntryHeader{CommitTime: 3}, []byte("Key2"), []byte("Value2")},
			&Entry{&EntryHeader{CommitTime: 3}, []byte("Key1"), []byte("Value3")},
			&Entry{&EntryHeader{CommitTime: 7}, []byte("Key1"), []byte("Value4")},
			&Entry{&EntryHeader{CommitTime: 7}, []byte("Key2"), []byte("Value5")},
			&Entry{&EntryHeader{CommitTime: 13, Flags: Flag_TransactionEnd}, []byte("Key3"), []byte("Value7")},
		}

		serializedEntries := make([][]byte, len(testEntries))
		for i, entry := range testEntries {
			serializedEntries[i] = SerializeEntry(entry)
		}

		entryStream := ConcatSliceList(serializedEntries)

		err := index.AppendFromEntryStream(bytes.NewReader(entryStream), 0, int64(len(entryStream)), nil)

		Expect(err).To(BeNil())

		Expect(index.FindOffsetOfFirstEntryUpdatedAfter(0)).To(EqualNumber(0))
		Expect(index.FindOffsetOfFirstEntryUpdatedAfter(1)).To(EqualNumber(0))
		Expect(index.FindOffsetOfFirstEntryUpdatedAfter(2)).To(EqualNumber(len(serializedEntries[0])))
		Expect(index.FindOffsetOfFirstEntryUpdatedAfter(3)).To(EqualNumber(len(serializedEntries[0]) + len(serializedEntries[1]) + len(serializedEntries[2])))
		Expect(index.FindOffsetOfFirstEntryUpdatedAfter(4)).To(EqualNumber(len(serializedEntries[0]) + len(serializedEntries[1]) + len(serializedEntries[2])))
		Expect(index.FindOffsetOfFirstEntryUpdatedAfter(5)).To(EqualNumber(len(serializedEntries[0]) + len(serializedEntries[1]) + len(serializedEntries[2])))
		Expect(index.FindOffsetOfFirstEntryUpdatedAfter(6)).To(EqualNumber(len(serializedEntries[0]) + len(serializedEntries[1]) + len(serializedEntries[2])))
		Expect(index.FindOffsetOfFirstEntryUpdatedAfter(7)).To(EqualNumber(len(serializedEntries[0]) + len(serializedEntries[1]) + len(serializedEntries[2]) + len(serializedEntries[3]) + len(serializedEntries[4])))

		Expect(index.FindOffsetOfFirstEntryUpdatedAfter(13)).To(EqualNumber(-1))
	})

	It("Gives the latest updated timestamp", func() {
		index := NewDatastoreIndex()
		Expect(index.LatestTimestamp()).To(Equal(int64(-1)))

		testEntries := []*Entry{
			&Entry{&EntryHeader{CommitTime: 2, Flags: Flag_TransactionEnd}, []byte("Key1"), []byte("Value1")},
			&Entry{&EntryHeader{CommitTime: 3, Flags: Flag_TransactionEnd}, []byte("Key2"), []byte("Value2")},
			&Entry{&EntryHeader{CommitTime: 3, Flags: Flag_TransactionEnd}, []byte("Key1"), []byte("Value3")},
			&Entry{&EntryHeader{CommitTime: 7, Flags: Flag_TransactionEnd}, []byte("Key1"), []byte("Value4")},
			&Entry{&EntryHeader{CommitTime: 7, Flags: Flag_TransactionEnd}, []byte("Key2"), []byte("Value5")},
			&Entry{&EntryHeader{CommitTime: 13, Flags: Flag_TransactionEnd}, []byte("Key3"), []byte("Value7")},
		}

		serializedEntries := make([][]byte, len(testEntries))
		for i, entry := range testEntries {
			serializedEntries[i] = SerializeEntry(entry)
		}

		for i, _ := range serializedEntries {
			err := index.AppendFromEntryStream(bytes.NewReader(serializedEntries[i]), 0, int64(len(serializedEntries[i])), nil)
			Expect(err).To(BeNil())
			Expect(index.LatestTimestamp()).To(Equal(testEntries[i].Header.CommitTime))
		}
	})
})

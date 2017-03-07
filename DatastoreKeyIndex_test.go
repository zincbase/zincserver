package main

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DatastoreKeyIndex", func() {
	var serializedEntries [][]byte
	var serializedEntryOffsets []int64
	var entryStreamBytes []byte

	BeforeSuite(func() {
		serializedEntries = [][]byte{
			SerializeEntry(&Entry{&EntryHeader{CommitTime: 1}, []byte("Key1"), []byte("a")}),
			SerializeEntry(&Entry{&EntryHeader{CommitTime: 3}, []byte("Key2"), []byte("")}),
			SerializeEntry(&Entry{&EntryHeader{CommitTime: 3}, []byte("Key1"), []byte("abcd")}),
			SerializeEntry(&Entry{&EntryHeader{CommitTime: 4}, []byte("Key1"), []byte("efg")}),
			SerializeEntry(&Entry{&EntryHeader{CommitTime: 6}, []byte("Key2"), []byte("hijkl")}),
			SerializeEntry(&Entry{&EntryHeader{CommitTime: 7}, []byte("Key3"), []byte("m")}),
			SerializeEntry(&Entry{&EntryHeader{CommitTime: 9}, []byte("Key3"), []byte("nopqrst")}),
		}

		serializedEntryOffsets = make([]int64, len(serializedEntries))

		for i, _ := range serializedEntries {
			serializedEntryOffsets[i] = int64(len(ConcatSliceList(serializedEntries[0:i])))
		}

		entryStreamBytes = ConcatSliceList(serializedEntries)
	})

	It("Creates a new index and adds elements to it", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.Set([]byte("Key0"), 4444, 5555)
		keyIndex.Set([]byte("Key1"), 5555, 6666)

		result, exists := keyIndex.Get([]byte("Key0"))
		Expect(exists).To(BeTrue())
		Expect(result.StartOffset).To(EqualNumber(4444))
		Expect(result.EndOffset).To(EqualNumber(5555))

		result, exists = keyIndex.Get([]byte("Key1"))
		Expect(exists).To(BeTrue())
		Expect(result.StartOffset).To(EqualNumber(5555))
		Expect(result.EndOffset).To(EqualNumber(6666))

		result, exists = keyIndex.Get([]byte("Key2"))
		Expect(exists).To(BeFalse())
		Expect(result.StartOffset).To(EqualNumber(0))
		Expect(result.EndOffset).To(EqualNumber(0))
	})

	It("Indexes a stream of entries by key", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStreamBytes), 0, int64(len(entryStreamBytes)))

		_, exists := keyIndex.Get([]byte("Key0"))
		Expect(exists).To(BeFalse())

		result, exists := keyIndex.Get([]byte("Key1"))
		Expect(exists).To(BeTrue())
		Expect(result.StartOffset).To(Equal(serializedEntryOffsets[3]))

		result, exists = keyIndex.Get([]byte("Key2"))
		Expect(exists).To(BeTrue())
		Expect(result.StartOffset).To(Equal(serializedEntryOffsets[4]))

		result, exists = keyIndex.Get([]byte("Key3"))
		Expect(exists).To(BeTrue())
		Expect(result.StartOffset).To(Equal(serializedEntryOffsets[6]))
	})

	It("Indexes a set of serialized entries by key", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStreamBytes), 0, int64(len(entryStreamBytes)))

		result, err := keyIndex.CompactToByteArray(bytes.NewReader(entryStreamBytes), 0)

		Expect(err).To(BeNil())
		Expect(result).To(Equal(ConcatSlices(serializedEntries[3], serializedEntries[4], serializedEntries[6])))
	})

	It("Compacts entry stream bytes", func() {
		result, err := CompactEntryStreamBytes(entryStreamBytes)
		Expect(err).To(BeNil())
		Expect(result).To(Equal(ConcatSlices(serializedEntries[3], serializedEntries[4], serializedEntries[6])))
	})

	It("Calculates compacted size", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStreamBytes), 0, int64(len(entryStreamBytes)))

		Expect(keyIndex.GetCompactedSize()).To(EqualNumber(len(serializedEntries[3]) + len(serializedEntries[4]) + len(serializedEntries[6])))
	})

	It("Calculates compacted ranges", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStreamBytes), 0, int64(len(entryStreamBytes)))

		compactedRanges := keyIndex.GetCompactedRanges(0, false)
		Expect(len(compactedRanges)).To(Equal(3))
		Expect(compactedRanges[0].StartOffset).To(EqualNumber(serializedEntryOffsets[3]))
		Expect(compactedRanges[0].EndOffset).To(EqualNumber(serializedEntryOffsets[4]))

		Expect(compactedRanges[1].StartOffset).To(EqualNumber(serializedEntryOffsets[4]))
		Expect(compactedRanges[1].EndOffset).To(EqualNumber(serializedEntryOffsets[5]))

		Expect(compactedRanges[2].StartOffset).To(EqualNumber(serializedEntryOffsets[6]))
		Expect(compactedRanges[2].EndOffset).To(EqualNumber(len(entryStreamBytes)))
	})

	It("Calculates compacted ranges", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStreamBytes), 0, int64(len(entryStreamBytes)))

		compactedRanges := keyIndex.GetCompactedRanges(0, false)
		Expect(len(compactedRanges)).To(Equal(3))
		Expect(compactedRanges[0].StartOffset).To(EqualNumber(serializedEntryOffsets[3]))
		Expect(compactedRanges[0].EndOffset).To(EqualNumber(serializedEntryOffsets[4]))

		Expect(compactedRanges[1].StartOffset).To(EqualNumber(serializedEntryOffsets[4]))
		Expect(compactedRanges[1].EndOffset).To(EqualNumber(serializedEntryOffsets[5]))

		Expect(compactedRanges[2].StartOffset).To(EqualNumber(serializedEntryOffsets[6]))
		Expect(compactedRanges[2].EndOffset).To(EqualNumber(len(entryStreamBytes)))
	})

	It("Calculates consolidated compacted ranges", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStreamBytes), 0, int64(len(entryStreamBytes)))

		compactedRanges := keyIndex.GetCompactedRanges(0, true)
		Expect(len(compactedRanges)).To(Equal(2))
		Expect(compactedRanges[0].StartOffset).To(EqualNumber(serializedEntryOffsets[3]))
		Expect(compactedRanges[0].EndOffset).To(EqualNumber(serializedEntryOffsets[5]))

		Expect(compactedRanges[1].StartOffset).To(EqualNumber(serializedEntryOffsets[6]))
		Expect(compactedRanges[1].EndOffset).To(EqualNumber(len(entryStreamBytes)))
	})

	It("Calculates compacted ranges from a starting offset", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStreamBytes), 0, int64(len(entryStreamBytes)))

		compactedRanges := keyIndex.GetCompactedRanges(serializedEntryOffsets[4], false)
		Expect(len(compactedRanges)).To(Equal(2))
		Expect(compactedRanges[0].StartOffset).To(EqualNumber(serializedEntryOffsets[4]))
		Expect(compactedRanges[0].EndOffset).To(EqualNumber(serializedEntryOffsets[5]))

		Expect(compactedRanges[1].StartOffset).To(EqualNumber(serializedEntryOffsets[6]))
		Expect(compactedRanges[1].EndOffset).To(EqualNumber(len(entryStreamBytes)))
	})
})

package main

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	//"log"
)

var _ = Describe("DatastoreKeyIndex", func() {
	var serializedEntries [][]byte
	var serializedEntryOffsets []int64
	var entryStream []byte

	BeforeSuite(func() {
		serializedEntries = [][]byte{
			SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 1}, []byte("Key1"), []byte("a")}),
			SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 3}, []byte("Key2"), []byte("")}),
			SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 3}, []byte("Key1"), []byte("abcd")}),
			SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 4}, []byte("Key1"), []byte("efg")}),
			SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 6}, []byte("Key2"), []byte("hijkl")}),
			SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 7}, []byte("Key3"), []byte("m")}),
			SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 9}, []byte("Key3"), []byte("nopqrst")}),
		}

		serializedEntryOffsets = make([]int64, len(serializedEntries))

		for i, _ := range serializedEntries {
			serializedEntryOffsets[i] = int64(len(ConcatBufferList(serializedEntries[0:i])))
		}

		entryStream = ConcatBufferList(serializedEntries)
	})

	It("Creates a new index and adds elements to it", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.Set([]byte("Key0"), 4444, 5555)
		keyIndex.Set([]byte("Key1"), 5555, 6666)

		result, exists := keyIndex.Get([]byte("Key0"))
		Expect(exists).To(BeTrue())
		Expect(result.StartOffset).To(Equal(int64(4444)))
		Expect(result.EndOffset).To(Equal(int64(5555)))

		result, exists = keyIndex.Get([]byte("Key1"))
		Expect(exists).To(BeTrue())
		Expect(result.StartOffset).To(Equal(int64(5555)))
		Expect(result.EndOffset).To(Equal(int64(6666)))

		result, exists = keyIndex.Get([]byte("Key2"))
		Expect(exists).To(BeFalse())
		Expect(result.StartOffset).To(Equal(int64(0)))
		Expect(result.EndOffset).To(Equal(int64(0)))
	})

	It("Indexes a stream of entries by key", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStream), 0, int64(len(entryStream)))

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
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStream), 0, int64(len(entryStream)))

		result, err := keyIndex.CompactToBuffer(bytes.NewReader(entryStream), 0)

		Expect(err).To(BeNil())
		Expect(result).To(Equal(ConcatBuffers(serializedEntries[3], serializedEntries[4], serializedEntries[6])))
	})

	It("Calculates compacted size", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStream), 0, int64(len(entryStream)))

		Expect(int(keyIndex.GetCompactedSize())).To(Equal(len(serializedEntries[3]) + len(serializedEntries[4]) + len(serializedEntries[6])))
	})

	It("Calculates compacted ranges", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStream), 0, int64(len(entryStream)))

		compactedRanges := keyIndex.GetCompactedRanges(0, false)
		Expect(len(compactedRanges)).To(Equal(3))
		Expect(compactedRanges[0].StartOffset).To(Equal(int64(serializedEntryOffsets[3])))
		Expect(compactedRanges[0].EndOffset).To(Equal(int64(serializedEntryOffsets[4])))

		Expect(compactedRanges[1].StartOffset).To(Equal(int64(serializedEntryOffsets[4])))
		Expect(compactedRanges[1].EndOffset).To(Equal(int64(serializedEntryOffsets[5])))

		Expect(compactedRanges[2].StartOffset).To(Equal(int64(serializedEntryOffsets[6])))
		Expect(compactedRanges[2].EndOffset).To(Equal(int64(len(entryStream))))
	})

	It("Calculates compacted ranges", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStream), 0, int64(len(entryStream)))

		compactedRanges := keyIndex.GetCompactedRanges(0, false)
		Expect(len(compactedRanges)).To(Equal(3))
		Expect(compactedRanges[0].StartOffset).To(Equal(int64(serializedEntryOffsets[3])))
		Expect(compactedRanges[0].EndOffset).To(Equal(int64(serializedEntryOffsets[4])))

		Expect(compactedRanges[1].StartOffset).To(Equal(int64(serializedEntryOffsets[4])))
		Expect(compactedRanges[1].EndOffset).To(Equal(int64(serializedEntryOffsets[5])))

		Expect(compactedRanges[2].StartOffset).To(Equal(int64(serializedEntryOffsets[6])))
		Expect(compactedRanges[2].EndOffset).To(Equal(int64(len(entryStream))))
	})

	It("Calculates consolidated compacted ranges", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStream), 0, int64(len(entryStream)))

		compactedRanges := keyIndex.GetCompactedRanges(0, true)
		Expect(len(compactedRanges)).To(Equal(2))
		Expect(compactedRanges[0].StartOffset).To(Equal(int64(serializedEntryOffsets[3])))
		Expect(compactedRanges[0].EndOffset).To(Equal(int64(serializedEntryOffsets[5])))

		Expect(compactedRanges[1].StartOffset).To(Equal(int64(serializedEntryOffsets[6])))
		Expect(compactedRanges[1].EndOffset).To(Equal(int64(len(entryStream))))
	})

	It("Calculates compacted ranges from a starting offset", func() {
		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(entryStream), 0, int64(len(entryStream)))

		compactedRanges := keyIndex.GetCompactedRanges(serializedEntryOffsets[4], false)
		Expect(len(compactedRanges)).To(Equal(2))
		Expect(compactedRanges[0].StartOffset).To(Equal(int64(serializedEntryOffsets[4])))
		Expect(compactedRanges[0].EndOffset).To(Equal(int64(serializedEntryOffsets[5])))

		Expect(compactedRanges[1].StartOffset).To(Equal(int64(serializedEntryOffsets[6])))
		Expect(compactedRanges[1].EndOffset).To(Equal(int64(len(entryStream))))
	})
})

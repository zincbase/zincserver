package main

import (
    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
	"bytes"
	"log"
)

var _ = Describe("DatastoreKeyIndex", func() {
	It("Indexes a set of entries", func() {
		entry1 := SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 1}, []byte("Key1"), []byte("Value1")})
		entry2 := SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 2}, []byte("Key2"), []byte("Value2")})
		entry3 := SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 3}, []byte("Key1"), []byte("Value3")})
		entry4 := SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 4}, []byte("Key1"), []byte("Value4")})
		entry5 := SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 5}, []byte("Key2"), []byte("Value5")})
		entry6 := SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 6}, []byte("Key3"), []byte("Value6")})
		entry7 := SerializeEntry(&Entry{&EntryPrimaryHeader{CommitTime: 7}, []byte("Key3"), []byte("Value7")})

		testData := ConcatBuffers(entry1, entry2, entry3, entry4, entry5, entry6, entry7)

		keyIndex := NewDatastoreKeyIndex()
		keyIndex.AddFromEntryStream(bytes.NewReader(testData), 0, int64(len(testData)))
		log.Println(keyIndex.GetCompactedRanges(0, true))
		result, err := keyIndex.CompactToBuffer(bytes.NewReader(testData), 0)

		Expect(err).To(BeNil())
		Expect(result).To(Equal(ConcatBuffers(entry4, entry5, entry7)))		
	})
})

package main

import (
	"bytes"
	"encoding/binary"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EntrySerializer", func() {
	It("Serializes and deserializes an entry", func() {
		testEntry := Entry{
			Header: &EntryHeader{
				KeyFormat:   DataFormat_UTF8,
				ValueFormat: DataFormat_Binary,
				Flags:       Flag_TransactionEnd,
				UpdateTime:  23422523422341542,
				CommitTime:  23422523422343423,
				HeaderChecksum: 3316190138, // Bogus checksum for testing only
				PayloadChecksum:       2042592394, // Bogus checksum for testing only
			},
			Key:                  []byte(RandomWordString(10)),
			Value:                []byte(RandomBytes(50)),
		}

		serializedEntry := SerializeEntry(&testEntry)
		deserializedEntry := DeserializeEntry(serializedEntry)

		ExpectEntriesToBeEqual(*deserializedEntry, testEntry)
	})

	It("Adds checksums that are verified correctly", func() {
		testEntry := Entry{
			Header: &EntryHeader{
				KeyFormat:   DataFormat_UTF8,
				ValueFormat: DataFormat_Binary,
				Flags:       Flag_TransactionEnd,
				UpdateTime:  23422523422341542,
				CommitTime:  23422523422343423,
				HeaderChecksum: 0,
				PayloadChecksum:       0,
			},
			Key:                  []byte(RandomWordString(10)),
			Value:                []byte(RandomBytes(50)),
		}

		serializedEntry := SerializeEntry(&testEntry)

		AddChecksumsToSerializedEntry(serializedEntry)
		Expect(binary.LittleEndian.Uint32(serializedEntry[32:36])).To(BeNumerically(">", 0))
		Expect(binary.LittleEndian.Uint32(serializedEntry[36:40])).To(BeNumerically(">", 0))

		Expect(VerifyHeaderChecksum(serializedEntry)).To(BeNil())
		Expect(VerifyPayloadChecksum(serializedEntry, bytes.NewReader(serializedEntry[40:]))).To(BeNil())
	})

	It("Compacts a series of entries", func() {
		entries := []Entry{
			Entry{Header: nil, Key: []byte("Key1"), Value: []byte("Value1")},
			Entry{Header: nil, Key: []byte("Key2"), Value: []byte("Value2")},
			Entry{Header: nil, Key: []byte("Key1"), Value: []byte("Value3")},
			Entry{Header: nil, Key: []byte("Key3"), Value: []byte("Value4")},
			Entry{Header: nil, Key: []byte("Key3"), Value: []byte("Value5")},
			Entry{Header: nil, Key: []byte("Key4"), Value: []byte("Value6")},
		}

		Expect(CompactEntries(entries)).To(Equal([]Entry{entries[1], entries[2], entries[4], entries[5]}))
	})
})

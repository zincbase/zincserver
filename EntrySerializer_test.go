package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"encoding/binary"
	"bytes"
)

var _ = Describe("EntrySerializer", func() {
	It("Serializes and deserializes an entry", func() {
		testEntry := Entry{
			PrimaryHeader: &EntryPrimaryHeader{
				UpdateTime:  23422523422341542,
				CommitTime:  23422523422343423,
				KeyFormat:   DataFormat_UTF8,
				ValueFormat: DataFormat_Binary,
				Flags:       Flag_TransactionEnd,
				PrimaryHeaderChecksum: 3316190138, // Bogus checksum for testing only
				PayloadChecksum:       2042592394, // Bogus checksum for testing only
			},
			SecondaryHeaderBytes: []byte(RandomBytes(20)),
			Key:                  []byte(RandomWordString(10)),
			Value:                []byte(RandomBytes(50)),
		}

		serializedEntry := SerializeEntry(&testEntry)
		deserializedEntry := DeserializeEntry(serializedEntry)

		ExpectEntriesToBeEqual(*deserializedEntry, testEntry)
	})

	It("Adds checksums that are verified correctly", func() {
		testEntry := Entry{
			PrimaryHeader: &EntryPrimaryHeader{
				UpdateTime:  23422523422341542,
				CommitTime:  23422523422343423,
				KeyFormat:   DataFormat_UTF8,
				ValueFormat: DataFormat_Binary,
				Flags:       Flag_TransactionEnd,
				PrimaryHeaderChecksum: 0,
				PayloadChecksum:       0,
			},
			SecondaryHeaderBytes: []byte(RandomBytes(20)),
			Key:                  []byte(RandomWordString(10)),
			Value:                []byte(RandomBytes(50)),
		}

		serializedEntry := SerializeEntry(&testEntry)

		AddChecksumsToSerializedEntry(serializedEntry)
		Expect(binary.LittleEndian.Uint32(serializedEntry[32:36])).To(BeNumerically(">", 0))
		Expect(binary.LittleEndian.Uint32(serializedEntry[36:40])).To(BeNumerically(">", 0))

		Expect(VerifyPrimaryHeaderChecksum(serializedEntry)).To(BeTrue())
		Expect(VerifyPayloadChecksum(serializedEntry, bytes.NewReader(serializedEntry[40:]))).To(BeTrue())
	})	
})

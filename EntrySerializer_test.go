package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EntrySerializer", func() {
	It("Serializes and deserializes a primary header", func() {
		testHeader := &EntryPrimaryHeader{
			TotalSize:           33452341,
			CommitTime:          345343452345,
			KeySize:             3945,
			KeyFormat:           54,
			ValueFormat:         23,
			EncryptionMethod:    34,
			Flags:               41,
			SecondaryHeaderSize: 5436,
		}

		serializedHeader := make([]byte, PrimaryHeaderSize)
		SerializePrimaryHeader(serializedHeader, testHeader)
		deserializedHeader := DeserializePrimaryHeader(serializedHeader)

		Expect(*testHeader).To(Equal(*deserializedHeader))
	})

	It("Serializes and deserializes an entry", func() {
		testEntry := Entry{
			PrimaryHeader: &EntryPrimaryHeader{
				UpdateTime:  54985934859843,
				CommitTime:  23422523422343423,
				KeyFormat:   DataFormat_UTF8,
				ValueFormat: DataFormat_UTF8,
				Flags:       Flag_TransactionEnd,
			},
			SecondaryHeaderBytes: []byte{},
			Key:                  []byte(RandomWordString(10)),
			Value:                []byte(RandomWordString(50)),
		}

		serializedEntry := SerializeEntry(&testEntry)
		deserializedEntry := DeserializeEntry(serializedEntry)

		Expect(*deserializedEntry).To(EqualEntry(testEntry))
	})

	It("Serializes and deserializes an entry containing a secondary header", func() {
		testEntry := Entry{
			PrimaryHeader: &EntryPrimaryHeader{
				UpdateTime:  54985934859843,
				CommitTime:  23422523422343423,
				KeyFormat:   DataFormat_UTF8,
				ValueFormat: DataFormat_UTF8,
				Flags:       Flag_TransactionEnd,
			},
			SecondaryHeaderBytes: []byte(RandomWordString(20)),
			Key:                  []byte(RandomWordString(10)),
			Value:                []byte(RandomWordString(50)),
		}

		serializedEntry := SerializeEntry(&testEntry)
		deserializedEntry := DeserializeEntry(serializedEntry)

		Expect(*deserializedEntry).To(EqualEntry(testEntry))
	})
})

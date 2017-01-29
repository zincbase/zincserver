package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EntrySerializer", func() {
	It("Serializes a primary header", func() {
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
		WritePrimaryHeader(serializedHeader, testHeader)
		deserializedHeader := ReadPrimaryHeader(serializedHeader)

		Expect(*testHeader).To(Equal(*deserializedHeader))
	})

	It("Deserializes a header", func() {
		testEntry := Entry{
			PrimaryHeader: &EntryPrimaryHeader{
				CommitTime:  23422523422343423,
				KeyFormat:   DataFormat_UTF8,
				ValueFormat: DataFormat_UTF8,
			},
			Key:   []byte(RandomWordString(60000)),
			Value: []byte(RandomWordString(100000)),
		}

		serializedEntry := SerializeEntry(&testEntry)
		deserializedEntry := DeserializeEntry(serializedEntry)

		Expect(*deserializedEntry).To(EqualEntry(testEntry))
	})
})

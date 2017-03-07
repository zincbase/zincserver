package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EntrySerializer", func() {
	It("Serializes and deserializes a header (architecture dependant implementation)", func() {
		testHeader := &EntryHeader{
			TotalSize:        33452341,
			HeaderVersion:    4321,
			KeySize:          3945,
			KeyFormat:        54,
			ValueFormat:      23,
			EncryptionMethod: 34,
			Flags:            41,
			UpdateTime:       345343452123,
			CommitTime:       345343452345,
			HeaderChecksum:   3316190138, // Bogus checksum for testing only
			PayloadChecksum:  2042592394, // Bogus checksum for testing only
		}

		serializedHeader := make([]byte, HeaderSize)
		SerializeHeader(testHeader, serializedHeader)
		deserializedHeader := DeserializeHeader(serializedHeader)

		Expect(*testHeader).To(Equal(*deserializedHeader))
	})

	It("Serializes and deserializes a header (slow implementation for BE architectures)", func() {
		testHeader := &EntryHeader{
			TotalSize:        33452341,
			HeaderVersion:    4321,
			KeySize:          3945,
			KeyFormat:        54,
			ValueFormat:      23,
			EncryptionMethod: 34,
			Flags:            41,
			UpdateTime:       345343452123,
			CommitTime:       345343452345,
			HeaderChecksum:   3316190138, // Bogus checksum for testing only
			PayloadChecksum:  2042592394, // Bogus checksum for testing only
		}

		serializedHeader := make([]byte, HeaderSize)
		SerializeHeader_Slow(testHeader, serializedHeader)
		deserializedHeader := DeserializeHeader_Slow(serializedHeader)

		Expect(*testHeader).To(Equal(*deserializedHeader))
	})
})

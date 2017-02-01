package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EntrySerializer", func() {
	It("Serializes and deserializes a primary header (architecture dependant implementation)", func() {
		testHeader := &EntryPrimaryHeader{
			TotalSize:             33452341,
			UpdateTime:            345343452123,
			CommitTime:            345343452345,
			KeySize:               3945,
			KeyFormat:             54,
			ValueFormat:           23,
			EncryptionMethod:      34,
			Flags:                 41,
			SecondaryHeaderSize:   5436,
			PrimaryHeaderChecksum: 3316190138, // Bogus checksum for testing only
			PayloadChecksum:       2042592394, // Bogus checksum for testing only
		}

		serializedHeader := make([]byte, PrimaryHeaderSize)
		SerializePrimaryHeader(serializedHeader, testHeader)
		deserializedHeader := DeserializePrimaryHeader(serializedHeader)

		Expect(*testHeader).To(Equal(*deserializedHeader))
	})

	It("Serializes and deserializes a primary header (slow implementation for BE architectures)", func() {
		testHeader := &EntryPrimaryHeader{
			TotalSize:             33452341,
			UpdateTime:            345343452123,
			CommitTime:            345343452345,
			KeySize:               3945,
			KeyFormat:             54,
			ValueFormat:           23,
			EncryptionMethod:      34,
			Flags:                 41,
			SecondaryHeaderSize:   5436,
			PrimaryHeaderChecksum: 3316190138, // Bogus checksum for testing only
			PayloadChecksum:       2042592394, // Bogus checksum for testing only
		}

		serializedHeader := make([]byte, PrimaryHeaderSize)
		SerializePrimaryHeader_Slow(serializedHeader, testHeader)
		deserializedHeader := DeserializePrimaryHeader_Slow(serializedHeader)

		Expect(*testHeader).To(Equal(*deserializedHeader))
	})
})

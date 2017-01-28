package main

import (
    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"	
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
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
		testEntry := Entry {
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

func EqualEntry(expected Entry) types.GomegaMatcher {
	return SatisfyAll(
		BeAssignableToTypeOf(Entry{nil, []byte{}, []byte{}}),
		MatchAllFields(Fields {
			"PrimaryHeader": Equal(expected.PrimaryHeader),
			"Key": Equal(expected.Key),
			"Value": Equal(expected.Value),
		}),
	)
}

func ExpectEntryArraysToBeEqual(entries1 []Entry, entries2 []Entry) bool {
	Expect(entries1).To(HaveLen(len(entries2)))

	for i := 0; i < len(entries1); i++ {
		Expect(entries1[i]).To(EqualEntry(entries2[i]))
	}

	return true
}

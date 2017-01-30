package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EntrySerializer", func() {
	It("Serializes and deserializes an entry", func() {
		testEntry := Entry{
			PrimaryHeader: &EntryPrimaryHeader{
				UpdateTime:  23422523422341542,
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

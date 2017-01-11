package main

import (
	"bytes"
	"log"
	"reflect"
	"testing"
)

func Test_EntrySerializer(test *testing.T) {
	test.Run("Header serialization", func(test *testing.T) {
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

		log.Println(testHeader)
		log.Println(serializedHeader)
		log.Println(deserializedHeader)

		if !reflect.DeepEqual(*testHeader, *deserializedHeader) {
			test.Error("Data mismatch.")
		}
	})

	test.Run("Entry serialization", func(test *testing.T) {
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

		if !EntriesAreEqual(deserializedEntry, &testEntry) {
			test.Error("Data mismatch.")
		}
	})

	test.Run("Formatted entry serialization (tabbedJson)", func(test *testing.T) {
		testEntries := []Entry{
			Entry{
				PrimaryHeader: &EntryPrimaryHeader{
					UpdateTime:  23422523422343423,
					CommitTime:  23422523422343423,
					KeyFormat:   DataFormat_JSON,
					ValueFormat: DataFormat_JSON,
				},
				Key:   []byte(`"Key1"`),
				Value: []byte(`"Value1"`),
			},
			Entry{
				PrimaryHeader: &EntryPrimaryHeader{
					UpdateTime:  23422523422343425,
					CommitTime:  23422523422343425,
					KeyFormat:   DataFormat_JSON,
					ValueFormat: DataFormat_JSON,
				},
				Key:   []byte(`"Key2"`),
				Value: []byte(`"Value2"`),
			},
			Entry{
				PrimaryHeader: &EntryPrimaryHeader{
					UpdateTime:  23422523422343427,
					CommitTime:  23422523422343427,
					KeyFormat:   DataFormat_JSON,
					ValueFormat: DataFormat_JSON,
				},
				Key:   []byte(`"Key3"`),
				Value: []byte(`"Value3"`),
			},
		}

		serializedEntries := SerializeEntries(testEntries)
		iterator := NewEntryStreamIterator(bytes.NewReader(serializedEntries), 0, int64(len(serializedEntries)), false)
		formatter := NewEntryStreamFormatter(iterator, "tabbedJson")
		formattedEntries, err := ReadCompleteStream(formatter)
		if err != nil {
			test.Error(err)
		}

		parsedEntries, err := ParseFormattedEntries(formattedEntries, "tabbedJson")
		if err != nil {
			test.Error(err)
		}
		log.Println(parsedEntries)

		if !EntryArraysAreEqual(parsedEntries, testEntries) {
			test.Error("Data mismatch.")
		}
	})
}

func EntriesAreEqual(entry1 *Entry, entry2 *Entry) bool {
	if !reflect.DeepEqual(entry1.Key, entry2.Key) {
		return false
	}

	if !reflect.DeepEqual(entry1.Value, entry2.Value) {
		return false
	}

	if (entry1.PrimaryHeader == nil && entry2.PrimaryHeader != nil) || (entry1.PrimaryHeader != nil && entry2.PrimaryHeader == nil) {
		return false
	}

	if entry1.PrimaryHeader == nil {
		return true
	}

	if !reflect.DeepEqual(*entry1.PrimaryHeader, *entry2.PrimaryHeader) {
		log.Println(*entry1.PrimaryHeader)
		log.Println(*entry2.PrimaryHeader)
		return false
	}

	return true
}

func EntryArraysAreEqual(entries1 []Entry, entries2 []Entry) bool {
	if len(entries1) != len(entries2) {
		return false
	}

	for i := 0; i < len(entries1); i++ {
		if !EntriesAreEqual(&entries1[i], &entries2[i]) {
			return false
		}
	}

	return true
}

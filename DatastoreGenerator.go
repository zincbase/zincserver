package main

import (
	"log"
)

func GenerateRandomDatastore(filePath string, entryCount int, keySize int, valueSize int, entryType string) (err error) {
	startTime := MonoUnixTimeMilliFloat()
	creationTimestamp := MonoUnixTimeMicro()

	entryStreamBuffer := GenerateRandomEntryStreamBytes(entryCount, keySize, valueSize, entryType)
	ValidateAndPrepareTransaction(entryStreamBuffer, -1)

	err = CreateOrRewriteFileSafe(filePath, CreateNewDatastoreReaderFromBytes(entryStreamBuffer, creationTimestamp))
	if err != nil {
		log.Printf("Failed storing '%s'\n", filePath)
		return
	}

	log.Printf("Datastore '%s' generated in %.3fms\n", filePath, MonoUnixTimeMilliFloat()-startTime)
	return
}

func GenerateRandomEntryStreamBytes(entryCount int, keySize int, valueSize int, entryType string) []byte {
	return SerializeEntries(GenerateRandomEntries(entryCount,keySize,valueSize, entryType))
}

func GenerateRandomEntries(entryCount int, keySize int, valueSize int, entryType string) []Entry {
	entries := []Entry{}

	for i := 0; i < entryCount; i++ {
		var entry *Entry

		switch entryType {
		case "randomPathEntry":
			entry = getRandomPathEntry(keySize, valueSize)
		case "randomPathEntryWithBinaryValue":
			entry = getRandomPathEntryWithBinaryValue(keySize, valueSize)
		case "randomUTF8Entry":
			entry = getRandomUtf8Entry(keySize, valueSize)
		case "randomBinaryEntry":
			entry = getRandomBinaryEntry(keySize, valueSize)
		case "randomAlphanumericEntry":
			entry = getRandomAlphanumericEntry(keySize, valueSize)
		case "randomJSONEntry":
			entry = getRandomJsonEntry(keySize, valueSize)
		default:
			panic("Invalid entry type requested: '" + entryType + "'")
		}

		entries = append(entries, *entry)
	}

	return entries
}

func getRandomUtf8Entry(keySize int, valueSize int) (result *Entry) {
	timestamp := MonoUnixTimeMicro()

	return &Entry{
		PrimaryHeader: &EntryPrimaryHeader{
			UpdateTime:  timestamp,
			CommitTime:  timestamp,
			KeyFormat:   DataFormat_UTF8,
			ValueFormat: DataFormat_UTF8,
			Flags:       Flag_TransactionEnd,
		},
		SecondaryHeaderBytes: []byte{},
		Key:   []byte(RandomUtf8String(keySize, -1)),
		Value: []byte(RandomUtf8String(valueSize, -1)),
	}
}

func getRandomBinaryEntry(keySize int, valueSize int) (result *Entry) {
	timestamp := MonoUnixTimeMicro()

	result = &Entry{
		PrimaryHeader: &EntryPrimaryHeader{
			UpdateTime:  timestamp,
			CommitTime:  timestamp,
			KeyFormat:   DataFormat_Binary,
			ValueFormat: DataFormat_Binary,
			Flags:       Flag_TransactionEnd,
		},
		SecondaryHeaderBytes: []byte{},
		Key:   RandomBytes(keySize),
		Value: RandomBytes(valueSize),
	}

	return
}

func getRandomAlphanumericEntry(keySize int, valueSize int) (result *Entry) {
	timestamp := MonoUnixTimeMicro()

	result = &Entry{
		PrimaryHeader: &EntryPrimaryHeader{
			UpdateTime:  timestamp,
			CommitTime:  timestamp,
			KeyFormat:   DataFormat_UTF8,
			ValueFormat: DataFormat_UTF8,
			Flags:       Flag_TransactionEnd,
		},
		SecondaryHeaderBytes: []byte{},
		Key:   []byte(RandomWordString(keySize)),
		Value: []byte(RandomWordString(valueSize)),
	}

	return
}

func getRandomJsonEntry(keySize int, valueSize int) (result *Entry) {
	timestamp := MonoUnixTimeMicro()

	result = &Entry{
		PrimaryHeader: &EntryPrimaryHeader{
			UpdateTime:  timestamp,
			CommitTime:  timestamp,
			KeyFormat:   DataFormat_JSON,
			ValueFormat: DataFormat_JSON,
			Flags:       Flag_TransactionEnd,
		},

		Key:   []byte(`"` + RandomWordString(keySize-2) + `"`),
		Value: []byte(`"` + RandomWordString(valueSize-2) + `"`),
	}

	return
}

func getRandomPathEntry(keySize int, valueSize int) (result *Entry) {
	timestamp := MonoUnixTimeMicro()

	result = &Entry{
		PrimaryHeader: &EntryPrimaryHeader{
			UpdateTime:  timestamp,
			CommitTime:  timestamp,
			KeyFormat:   DataFormat_JSON,
			ValueFormat: DataFormat_JSON,
			Flags:       Flag_TransactionEnd,
		},
		SecondaryHeaderBytes: []byte{},
		Key:   []byte(`"['` + RandomWordString(keySize-6) + `']"`),
		Value: []byte(`"` + RandomWordString(valueSize-2) + `"`),
	}

	return
}

func getRandomPathEntryWithBinaryValue(keySize int, valueSize int) (result *Entry) {
	timestamp := MonoUnixTimeMicro()

	result = &Entry{
		PrimaryHeader: &EntryPrimaryHeader{
			UpdateTime:  timestamp,
			CommitTime:  timestamp,
			KeyFormat:   DataFormat_JSON,
			ValueFormat: DataFormat_Binary,
			Flags:       Flag_TransactionEnd,
		},
		SecondaryHeaderBytes: []byte{},
		Key:   []byte(`"['` + RandomWordString(keySize-6) + `']"`),
		Value: RandomBytes(valueSize),
	}

	return
}

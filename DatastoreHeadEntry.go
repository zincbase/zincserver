package main

import (
	"bytes"
	"encoding/binary"
	"io"
)

const DatastoreVersion = 1
const HeadEntrySize = 512
const HeadEntryValueSize = HeadEntrySize - HeaderSize

type HeadEntryValue struct {
	Version                       int64
	LastCompactionTime            int64
	LastCompactionCheckTime       int64
	LastCompactionCheckSize       int64
	LastCompactionCheckUnusedSize int64
}

////////////////////////////////////////////////////////////////////////////////
// Head entry serialization
////////////////////////////////////////////////////////////////////////////////

// Create a serialized head entry, given a value object and timestamp, to be
// used as a creation timestamp
func CreateSerializedHeadEntry(value *HeadEntryValue, timestamp int64) (serializedHeadEntry []byte) {
	// Serialize the given head entry
	serializedHeadEntry = SerializeEntry(CreateHeadEntry(value, timestamp))

	// Add checksums to the serialized entry
	AddChecksumsToSerializedEntry(serializedHeadEntry)

	return
}

// Create a head entry object, given a value object and a timestamp
func CreateHeadEntry(value *HeadEntryValue, timestamp int64) *Entry {
	// If no timestamp was given
	if timestamp <= 0 {
		// Take the current time
		timestamp = MonoUnixTimeMicro()
	}

	// Return an entry with the timestamp and serialized value
	return &Entry{
		Header:        &EntryHeader{
			UpdateTime: timestamp,
			CommitTime: timestamp,
			Flags: Flag_TransactionEnd | Flag_HeadEntry,
		},
		Key:                  []byte{},
		Value:                SerializeHeadEntryValue(value),
	}
}

// Serialize the value of a head entry
func SerializeHeadEntryValue(value *HeadEntryValue) (serializedMetadataEntryValue []byte) {
	serializedMetadataEntryValue = make([]byte, HeadEntryValueSize)

	binary.LittleEndian.PutUint64(serializedMetadataEntryValue[0:8], uint64(value.Version))
	binary.LittleEndian.PutUint64(serializedMetadataEntryValue[8:16], uint64(value.LastCompactionTime))
	binary.LittleEndian.PutUint64(serializedMetadataEntryValue[16:24], uint64(value.LastCompactionCheckTime))
	binary.LittleEndian.PutUint64(serializedMetadataEntryValue[24:32], uint64(value.LastCompactionCheckSize))
	binary.LittleEndian.PutUint64(serializedMetadataEntryValue[32:40], uint64(value.LastCompactionCheckUnusedSize))

	return
}

////////////////////////////////////////////////////////////////////////////////
// Head entry deserialization
////////////////////////////////////////////////////////////////////////////////

// Deserialize a head entry value
func DeserializeHeadEntryValue(valueBytes []byte) *HeadEntryValue {
	return &HeadEntryValue{
		Version:                       int64(binary.LittleEndian.Uint64(valueBytes[0:8])),
		LastCompactionTime:            int64(binary.LittleEndian.Uint64(valueBytes[8:16])),
		LastCompactionCheckTime:       int64(binary.LittleEndian.Uint64(valueBytes[16:24])),
		LastCompactionCheckSize:       int64(binary.LittleEndian.Uint64(valueBytes[24:32])),
		LastCompactionCheckUnusedSize: int64(binary.LittleEndian.Uint64(valueBytes[32:40])),
	}
}

////////////////////////////////////////////////////////////////////////////////
// Datastore creation
////////////////////////////////////////////////////////////////////////////////

// Create a stream for a new datastore, given a reader stream for the content and a creation
// timestamp
func CreateNewDatastoreReader(newDatastoreContentReader io.Reader, creationTimestamp int64) io.Reader {
	serializedHeadEntry := CreateSerializedHeadEntry(&HeadEntryValue{Version: DatastoreVersion}, creationTimestamp)
	return io.MultiReader(bytes.NewReader(serializedHeadEntry), newDatastoreContentReader)
}

// Create a reader for a new datastore, given a slice of bytes as content and a timestamp
func CreateNewDatastoreReaderFromBytes(newDatastoreContentBytes []byte, creationTimestamp int64) io.Reader {
	return CreateNewDatastoreReader(bytes.NewReader(newDatastoreContentBytes), creationTimestamp)
}

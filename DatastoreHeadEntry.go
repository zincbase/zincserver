package main

import (
	"bytes"
	"encoding/binary"
	"io"
)

const DatastoreVersion = 1
const HeadEntrySize = 512
const HeadEntryValueSize = HeadEntrySize - PrimaryHeaderSize

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
func CreateSerializedHeadEntry(value *HeadEntryValue, timestamp int64) (serializedHeadEntry []byte) {
	serializedHeadEntry = SerializeEntry(CreateHeadEntry(value, timestamp))
	AddChecksumsToSerializedEntry(serializedHeadEntry)

	return
}

func CreateHeadEntry(value *HeadEntryValue, timestamp int64) *Entry {
	if timestamp <= 0 {
		timestamp = MonoUnixTimeMicro()
	}

	return &Entry{
		PrimaryHeader:        &EntryPrimaryHeader{ UpdateTime: timestamp, CommitTime: timestamp, Flags: Flag_TransactionEnd},
		SecondaryHeaderBytes: []byte{},
		Key:                  []byte{},
		Value:                SerializeHeadEntryValue(value),
	}
}

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
func CreateNewDatastoreReader(newDatastoreContentReader io.Reader, creationTimestamp int64) io.Reader {
	serializedHeadEntry := CreateSerializedHeadEntry(&HeadEntryValue{Version: DatastoreVersion}, creationTimestamp)
	return io.MultiReader(bytes.NewReader(serializedHeadEntry), newDatastoreContentReader)
}

func CreateNewDatastoreReaderFromBytes(newDatastoreContentBytes []byte, creationTimestamp int64) io.Reader {
	return CreateNewDatastoreReader(bytes.NewReader(newDatastoreContentBytes), creationTimestamp)
}

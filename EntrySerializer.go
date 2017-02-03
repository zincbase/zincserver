package main

import (
	"bytes"
	"errors"
	//"strconv"
	"encoding/binary"
	"io"
)

const PrimaryHeaderSize = 40

type Entry struct {
	PrimaryHeader        *EntryPrimaryHeader
	SecondaryHeaderBytes []byte
	Key                  []byte
	Value                []byte
}

type EntryPrimaryHeader struct {
	TotalSize             int64  // [0:8]
	UpdateTime            int64  // [8:16]
	CommitTime            int64  // [16:24]
	KeySize               uint16 // [24:26]
	KeyFormat             uint8  // [26]
	ValueFormat           uint8  // [27]
	EncryptionMethod      uint8  // [28]
	Flags                 uint8  // [29]
	SecondaryHeaderSize   uint16 // [30:32]
	PrimaryHeaderChecksum uint32 // [32:36] CRC32C checksum of the primary header (bytes [0:32] only)
	PayloadChecksum       uint32 // [36:30] CRC32C checksum of the rest of the entry (bytes [40:TotalSize])
}

const (
	Flag_TransactionEnd uint8 = 1
	DataFormat_Binary   uint8 = 0
	DataFormat_UTF8     uint8 = 1
	DataFormat_JSON     uint8 = 2
	DataFormat_OmniJSON uint8 = 3
)

type JsonEntry struct {
	key   string
	value string
}

////////////////////////////////////////////////////////////////////////////////
// Serialization
////////////////////////////////////////////////////////////////////////////////
func SerializeEntries(entries []Entry) []byte {
	memoryWriter := NewMemoryWriter()

	for i := 0; i < len(entries); i++ {
		memoryWriter.Write(SerializeEntry(&entries[i]))
	}

	return memoryWriter.WrittenData()
}

func SerializeEntry(entry *Entry) (serializedEntry []byte) {
	// Calculate sizes of the entry's components
	secondaryHeaderSize := uint16(len(entry.SecondaryHeaderBytes))
	keySize := uint16(len(entry.Key))
	valueSize := int64(len(entry.Value))
	totalSize := int64(PrimaryHeaderSize + int(secondaryHeaderSize) + int(keySize) + int(valueSize))

	// Fill out the primary header fields if needed
	if entry.PrimaryHeader == nil {
		entry.PrimaryHeader = &EntryPrimaryHeader{}
	}

	entry.PrimaryHeader.TotalSize = totalSize
	if entry.PrimaryHeader.UpdateTime == 0 {
		entry.PrimaryHeader.UpdateTime = MonoUnixTimeMicro()
	}
	entry.PrimaryHeader.KeySize = keySize
	entry.PrimaryHeader.SecondaryHeaderSize = secondaryHeaderSize

	// Calculate offsets
	secondaryHeaderOffset := PrimaryHeaderSize
	keyOffset := int64(secondaryHeaderOffset) + int64(secondaryHeaderSize)
	valueOffset := keyOffset + int64(keySize)

	// Build the serialized entry
	serializedEntry = make([]byte, totalSize)

	SerializePrimaryHeader(serializedEntry[0:secondaryHeaderOffset], entry.PrimaryHeader)
	copy(serializedEntry[secondaryHeaderOffset:keyOffset], entry.SecondaryHeaderBytes)
	copy(serializedEntry[keyOffset:valueOffset], entry.Key)
	copy(serializedEntry[valueOffset:], entry.Value)

	return
}

func AddChecksumsToSerializedEntry(serializedEntry []byte) {
	// Calculate primary header checksum (this include only bytes 0..32 of the header, the rest is skipped)
	primaryHeaderChecksum := CRC32C(serializedEntry[0:32])

	// Add the primary header checksum to the serialized entry
	binary.LittleEndian.PutUint32(serializedEntry[32:36], primaryHeaderChecksum)

	// Calculate the payload's checksum
	payloadChecksum := CRC32C(serializedEntry[40:])

	// Add the payload checksum to the serialized entry
	binary.LittleEndian.PutUint32(serializedEntry[36:40], payloadChecksum)
}

func SerializeJsonEntries(jsonEntries []JsonEntry) []byte {
	entries := []Entry{}

	for _, jsonEntry := range jsonEntries {
		entries = append(entries, Entry{
			PrimaryHeader: &EntryPrimaryHeader{
				KeyFormat:   DataFormat_JSON,
				ValueFormat: DataFormat_JSON,
			},
			SecondaryHeaderBytes: []byte{},
			Key:                  []byte(jsonEntry.key),
			Value:                []byte(jsonEntry.value),
		})
	}

	return SerializeEntries(entries)
}

////////////////////////////////////////////////////////////////////////////////
// Deserialization
////////////////////////////////////////////////////////////////////////////////
func DeserializeEntryStreamBytes(entryStream []byte) ([]Entry, error) {
	return DeserializeEntryStreamReader(bytes.NewReader(entryStream), 0, int64(len(entryStream)))
}

func DeserializeEntryStreamReader(source io.ReaderAt, startOffset int64, endOffset int64) ([]Entry, error) {
	next := NewEntryStreamIterator(source, startOffset, endOffset)

	results := []Entry{}

	for {
		iteratorResult, err := next()

		if err != nil {
			return nil, err
		}

		if iteratorResult == nil {
			return results, err
		}

		secondaryHeaderBytes, err := iteratorResult.ReadSecondaryHeaderBytes()
		if err != nil {
			return nil, err
		}

		key, value, err := iteratorResult.ReadKeyAndValue()
		if err != nil {
			return nil, err
		}

		results = append(results, Entry{
			PrimaryHeader:        iteratorResult.PrimaryHeader,
			SecondaryHeaderBytes: secondaryHeaderBytes,
			Key:                  key,
			Value:                value,
		})
	}
}

func DeserializeEntry(entryBytes []byte) *Entry {
	return DeserializePrimaryHeaderAndRemainderBytes(entryBytes[0:PrimaryHeaderSize], entryBytes[PrimaryHeaderSize:])
}

func DeserializePrimaryHeaderAndRemainderBytes(primaryHeaderBytes []byte, remainderBytes []byte) *Entry {
	primaryHeader := DeserializePrimaryHeader(primaryHeaderBytes)

	secondaryHeaderOffset := int64(0)
	keyOffset := secondaryHeaderOffset + int64(primaryHeader.SecondaryHeaderSize)
	valueOffset := keyOffset + int64(primaryHeader.KeySize)

	return &Entry{
		PrimaryHeader:        primaryHeader,
		SecondaryHeaderBytes: remainderBytes[secondaryHeaderOffset:keyOffset],
		Key:                  remainderBytes[keyOffset:valueOffset],
		Value:                remainderBytes[valueOffset:],
	}
}

func DeserializeEntryStreamReaderAndAppendToVarMap(source io.ReaderAt, startOffset int64, endOffset int64, target *VarMap) (err error) {
	entries, err := DeserializeEntryStreamReader(source, startOffset, endOffset)
	if err != nil {
		return
	}

	err = target.AppendJsonEntries(entries)

	return
}

////////////////////////////////////////////////////////////////////////////////
// Validation
////////////////////////////////////////////////////////////////////////////////
func VerifyPrimaryHeaderChecksum(serializedHeader []byte) error {
	if len(serializedHeader) < PrimaryHeaderSize {
		return io.ErrUnexpectedEOF
	}

	// Deserialize the expected checksum from the header's bytes
	expectedChecksum := binary.LittleEndian.Uint32(serializedHeader[32:36])

	// Calculate the actual checksum
	actualChecksum := CRC32C(serializedHeader[0:32])

	// Return their comparison result
	if actualChecksum != expectedChecksum {
		return ErrCorruptedEntry
	}

	return nil
}

func VerifyPayloadChecksum(serializedHeader []byte, payloadReader io.Reader) error {
	// Deserialize the expected checksum from the header's bytes
	expectedChecksum := binary.LittleEndian.Uint32(serializedHeader[36:40])

	// Calculate the actual checksum
	actualChecksum, err := CRC32COfReader(payloadReader)

	// If an error occurred when calculating the CRC32 of the payload
	if err != nil {
		// Return the error
		return err
	}

	if actualChecksum != expectedChecksum {
		return ErrCorruptedEntry
	}

	return nil
}

func ValidateAndPrepareTransaction(entryStream []byte, newCommitTimestamp int64) error {
	const minAllowedTimestamp int64 = 1483221600 * 1000000
	maxAllowedTimestamp := MonoUnixTimeMicro() + (30 * 1000000)

	next := NewEntryStreamIterator(bytes.NewReader(entryStream), 0, int64(len(entryStream)))

	for {
		iteratorResult, err := next()

		if err != nil {
			return err
		}

		if iteratorResult == nil {
			return nil
		}

		if iteratorResult.PrimaryHeader.KeySize == 0 {
			return errors.New("Encountered an entry with a zero length key, which is not permitted in transaction entries.")
		}

		if iteratorResult.PrimaryHeader.Flags > 1 {
			return errors.New("Encountered an entry header containing a flag that is not 'TransactionEnd' (1).")
		}

		if iteratorResult.PrimaryHeader.UpdateTime < minAllowedTimestamp {
			return errors.New("Encountered an entry header containing an update time smaller than 1483221600 * 1000000 (Januaray 1st 2017, 00:00).")
		}

		if iteratorResult.PrimaryHeader.UpdateTime > maxAllowedTimestamp {
			return errors.New("Encountered an entry header containing an update time greater than 30 seconds past the server's clock.")
		}

		if newCommitTimestamp > 0 {
			iteratorResult.PrimaryHeader.CommitTime = newCommitTimestamp
		}

		if iteratorResult.Offset+iteratorResult.Size == int64(len(entryStream)) {
			iteratorResult.PrimaryHeader.Flags |= Flag_TransactionEnd
		}

		// Update the header bytes
		SerializePrimaryHeader(entryStream[iteratorResult.Offset:], iteratorResult.PrimaryHeader)

		// Add checksums for the header and payload
		AddChecksumsToSerializedEntry(entryStream[iteratorResult.Offset:iteratorResult.EndOffset()])
	}
}

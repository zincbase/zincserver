package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const HeaderSize = 40

type Entry struct {
	Header *EntryHeader
	Key    []byte
	Value  []byte
}

type EntryHeader struct {
	TotalSize        int64  // [0:8]
	HeaderVersion    uint16 // [8:10]
	KeySize          uint16 // [10:12]
	KeyFormat        uint8  // [12]
	ValueFormat      uint8  // [13]
	EncryptionMethod uint8  // [14]
	Flags            uint8  // [15]
	UpdateTime       int64  // [16:24]
	CommitTime       int64  // [24:32]
	HeaderChecksum   uint32 // [32:36] CRC32C checksum of the header (bytes [0:32] only)
	PayloadChecksum  uint32 // [36:40] CRC32C checksum of the rest of the entry (bytes [40:TotalSize])
}

const (
	Flag_TransactionEnd uint8 = 1
	Flag_HeadEntry      uint8 = 2
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
	keySize := uint16(len(entry.Key))
	valueSize := int64(len(entry.Value))
	totalSize := int64(HeaderSize + int(keySize) + int(valueSize))

	// Fill out the header fields if needed
	if entry.Header == nil {
		entry.Header = &EntryHeader{}
	}

	entry.Header.TotalSize = totalSize
	entry.Header.KeySize = keySize

	if entry.Header.UpdateTime == 0 {
		entry.Header.UpdateTime = MonoUnixTimeMicro()
	}

	// Calculate offsets
	keyOffset := int64(HeaderSize)
	valueOffset := keyOffset + int64(keySize)

	// Build the serialized entry
	serializedEntry = make([]byte, totalSize)

	SerializeHeader(entry.Header, serializedEntry[0:HeaderSize])
	copy(serializedEntry[keyOffset:valueOffset], entry.Key)
	copy(serializedEntry[valueOffset:], entry.Value)

	return
}

func AddChecksumsToSerializedEntry(serializedEntry []byte) {
	// Calculate header checksum (this include only bytes 0..32 of the header, the rest is skipped)
	headerChecksum := CRC32C(serializedEntry[0:32])

	// Add the header checksum to the serialized entry
	binary.LittleEndian.PutUint32(serializedEntry[32:36], headerChecksum)

	// Calculate the payload's checksum
	payloadChecksum := CRC32C(serializedEntry[HeaderSize:])

	// Add the payload checksum to the serialized entry
	binary.LittleEndian.PutUint32(serializedEntry[36:40], payloadChecksum)
}

func SerializeJsonEntries(jsonEntries []JsonEntry) []byte {
	entries := []Entry{}

	for _, jsonEntry := range jsonEntries {
		entries = append(entries, Entry{
			Header: &EntryHeader{
				KeyFormat:   DataFormat_JSON,
				ValueFormat: DataFormat_JSON,
			},
			Key:   []byte(jsonEntry.key),
			Value: []byte(jsonEntry.value),
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

		key, value, err := iteratorResult.ReadKeyAndValue()
		if err != nil {
			return nil, err
		}

		results = append(results, Entry{
			Header: iteratorResult.Header,
			Key:    key,
			Value:  value,
		})
	}
}

func DeserializeEntry(entryBytes []byte) *Entry {
	return DeserializeHeaderAndPayloadBytes(entryBytes[0:HeaderSize], entryBytes[HeaderSize:])
}

func DeserializeHeaderAndPayloadBytes(headerBytes []byte, payloadBytes []byte) *Entry {
	header := DeserializeHeader(headerBytes)

	return &Entry{
		Header: header,
		Key:    payloadBytes[0:header.KeySize],
		Value:  payloadBytes[header.KeySize:],
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
// Compaction
////////////////////////////////////////////////////////////////////////////////
func CompactEntries(entries []Entry) []Entry {
	results := []Entry{}
	seenKeys := make(map[string]bool)

	for i := len(entries) - 1; i >= 0; i-- {
		keyHash := SHA1ToString(entries[i].Key)

		if seenKeys[keyHash] == true {
			continue
		} else {
			seenKeys[keyHash] = true
			results = append(results, entries[i])
		}
	}

	// Reverse results
	left := 0
	right := len(results) - 1

	for left < right {
		results[left], results[right] = results[right], results[left]

		left += 1
		right -= 1
	}

	return results
}

////////////////////////////////////////////////////////////////////////////////
// Validation
////////////////////////////////////////////////////////////////////////////////
func VerifyHeaderChecksum(serializedHeader []byte) error {
	if len(serializedHeader) < HeaderSize {
		return io.ErrUnexpectedEOF
	}

	// Deserialize the expected checksum from the header's bytes
	expectedChecksum := binary.LittleEndian.Uint32(serializedHeader[32:36])

	// Calculate the actual checksum
	actualChecksum := CRC32C(serializedHeader[0:32])

	// If the actual checksum doesn't match the expected one
	if actualChecksum != expectedChecksum {
		// Return a corrupted entry error
		return ErrCorruptedEntry
	}

	return nil
}

func VerifyPayloadChecksum(serializedHeader []byte, payloadReader io.Reader) error {
	// Deserialize the expected checksum from the payload's bytes
	expectedChecksum := binary.LittleEndian.Uint32(serializedHeader[36:40])

	// Calculate the actual checksum
	actualChecksum, err := CRC32COfReader(payloadReader)

	// If an error occurred when calculating the CRC32 of the payload
	if err != nil {
		// Return the error
		return err
	}

	// If the actual checksum doesn't match the expected one
	if actualChecksum != expectedChecksum {
		// Return a corrupted entry error
		return ErrCorruptedEntry
	}

	return nil
}

func ValidateAndPrepareTransaction(entryStream []byte, newCommitTimestamp int64, maxEntrySize int64) error {
	// Initialize the minimal allowed timestamp to 01/01/2017
	const minAllowedTimestamp int64 = 1483221600 * 1000000
	// Set the maximal allowed timestamp to current time + 30 seconds
	maxAllowedTimestamp := MonoUnixTimeMicro() + (30 * 1000000)

	// Create an iterator to the given entry stream
	next := NewEntryStreamIterator(bytes.NewReader(entryStream), 0, int64(len(entryStream)))

	// Repeat
	for {
		// Iterate to next result
		iteratorResult, err := next()

		// If an error occurred when iterating
		if err != nil {
			// Return the error
			return err
		}

		// If the iterator result is empty
		if iteratorResult == nil {
			// Return without error
			return nil
		}

		// Ensure the key size isn't zero
		if iteratorResult.KeySize() == 0 {
			return ErrEntryRejected{"Encountered an entry with a zero length key, which is not permitted in transaction entries."}
		}

		// Ensure the entry size isn't greater than the maximum allowed
		if maxEntrySize > 0 && iteratorResult.Size > maxEntrySize {
			return ErrDatastoreEntrySizeLimitExceeded{fmt.Sprintf("Encountered an entry with a size of %d, which is greater than the maximum allowed (%d).", iteratorResult.Size, maxEntrySize)}
		}

		// Ensure the header doesn't contain any other flag than 'TransactionEnd'
		if iteratorResult.Header.Flags > 1 {
			return ErrEntryRejected{"Encountered an entry header containing a flag that is not 'TransactionEnd' (1)."}
		}

		// Ensure the entry's timestamp isn't less than than the minimum allowed timestamp
		if iteratorResult.UpdateTime() < minAllowedTimestamp {
			return ErrEntryRejected{"Encountered an entry header containing an update time lesser than 1483221600 * 1000000 (Januaray 1st 2017, 00:00)."}
		}

		// Ensure the entry's timestamp isn't greater than than the maximum allowed timestamp
		if iteratorResult.Header.UpdateTime > maxAllowedTimestamp {
			return ErrEntryRejected{"Encountered an entry header containing an update time greater than 30 seconds past the server's clock."}
		}

		// Set the commit timestamp, if needed
		if newCommitTimestamp > 0 {
			iteratorResult.Header.CommitTime = newCommitTimestamp
		}

		// If the entry is the last one
		if iteratorResult.EndOffset() == int64(len(entryStream)) {
			// Add a transaction end flag to it
			iteratorResult.Header.Flags |= Flag_TransactionEnd
		}

		// Update the serialized entry with the modified header
		SerializeHeader(iteratorResult.Header, entryStream[iteratorResult.Offset:])

		// Add checksums for both the header and payload
		AddChecksumsToSerializedEntry(entryStream[iteratorResult.Offset:iteratorResult.EndOffset()])
	}
}

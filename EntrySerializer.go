// +build amd64 386 arm arm64 ppc64le mips64le

package main

import (
	"bytes"
	"errors"
	//"strconv"
	"io"
	"unsafe"
)

const PrimaryHeaderSize = 32

type Entry struct {
	PrimaryHeader *EntryPrimaryHeader
	Key           []byte
	Value         []byte
}

type EntryPrimaryHeader struct {
	TotalSize           int64  // 0..7
	UpdateTime          int64  // 8..15
	CommitTime          int64  // 16..23
	KeySize             uint16 // 24..25
	KeyFormat           uint8  // 26
	ValueFormat         uint8  // 27
	EncryptionMethod    uint8  // 28
	Flags               uint8  // 29
	SecondaryHeaderSize uint16 // 30..31 For future use. Secondary headers are currently ignored.
}

const (
	Flag_TransactionEnd uint8 = 1
	Flag_CreationEvent  uint8 = 2
	DataFormat_Binary   uint8 = 0
	DataFormat_UTF8     uint8 = 1
	DataFormat_JSON     uint8 = 2
)

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
	totalSize := int64(PrimaryHeaderSize + len(entry.Key) + len(entry.Value))
	keySize := uint16(len(entry.Key))

	if entry.PrimaryHeader == nil {
		timestamp := MonoUnixTimeMicro()

		entry.PrimaryHeader = &EntryPrimaryHeader{
			TotalSize:  totalSize,
			CommitTime: timestamp,
			UpdateTime: timestamp,
			KeySize:    keySize,
		}
	} else {
		entry.PrimaryHeader.TotalSize = totalSize
		entry.PrimaryHeader.KeySize = keySize
	}

	serializedEntry = make([]byte, totalSize)

	WritePrimaryHeader(serializedEntry[0:PrimaryHeaderSize], entry.PrimaryHeader)

	copy(serializedEntry[PrimaryHeaderSize:PrimaryHeaderSize+keySize], entry.Key)
	copy(serializedEntry[PrimaryHeaderSize+keySize:], entry.Value)

	return
}

func WritePrimaryHeader(targetSlice []byte, header *EntryPrimaryHeader) {
	*(*EntryPrimaryHeader)(unsafe.Pointer(&targetSlice[0])) = *header
}

////////////////////////////////////////////////////////////////////////////////
// Deserialization
////////////////////////////////////////////////////////////////////////////////
func DeserializeEntryStreamBytes(entryStream []byte) ([]Entry, error) {
	return DeserializeEntryStreamReader(bytes.NewReader(entryStream), 0, int64(len(entryStream)))
}

func DeserializeEntryStreamReader(source io.ReaderAt, startOffset int64, endOffset int64) ([]Entry, error) {
	next := NewEntryStreamIterator(source, startOffset, endOffset, false)

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
			PrimaryHeader: iteratorResult.PrimaryHeader,
			Key:           key,
			Value:         value,
		})
	}
}

func DeserializeEntry(entryBytes []byte) *Entry {
	return DeserializePrimaryHeaderAndRemainderBytes(entryBytes[0:PrimaryHeaderSize], entryBytes[PrimaryHeaderSize:])
}

func DeserializePrimaryHeaderAndRemainderBytes(primaryHeaderBytes []byte, remainderBytes []byte) *Entry {
	primaryHeader := ReadPrimaryHeader(primaryHeaderBytes)

	return &Entry{
		PrimaryHeader: primaryHeader,
		Key:           remainderBytes[primaryHeader.SecondaryHeaderSize : primaryHeader.SecondaryHeaderSize+primaryHeader.KeySize],
		Value:         remainderBytes[primaryHeader.SecondaryHeaderSize+primaryHeader.KeySize:],
	}
}

func ReadPrimaryHeader(primaryHeaderBytes []byte) *EntryPrimaryHeader {
	return (*EntryPrimaryHeader)(unsafe.Pointer(&primaryHeaderBytes[0]))
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
func ValidateAndPrepareTransaction(entryStream []byte, newTimestamp int64) error {
	const minAllowedTimestamp int64 = 1483221600 * 1000000
	maxAllowedTimestamp := MonoUnixTimeMicro() + (30 * 1000000)

	next := NewEntryStreamIterator(bytes.NewReader(entryStream), 0, int64(len(entryStream)), false)

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

		if newTimestamp > 0 {
			iteratorResult.PrimaryHeader.CommitTime = newTimestamp
		}

		if iteratorResult.Offset+iteratorResult.Size == int64(len(entryStream)) {
			iteratorResult.PrimaryHeader.Flags |= Flag_TransactionEnd
		}

		WritePrimaryHeader(entryStream[iteratorResult.Offset:], iteratorResult.PrimaryHeader)
	}
}

////////////////////////////////////////////////////////////////////////////////
// Datastore creation
////////////////////////////////////////////////////////////////////////////////
func CreateNewDatastoreReader(newDatastoreContentReader io.Reader, creationTimestamp int64) io.Reader {
	creationEntry := SerializeEntry(&Entry{&EntryPrimaryHeader{UpdateTime: creationTimestamp, CommitTime: creationTimestamp, Flags: Flag_TransactionEnd | Flag_CreationEvent}, []byte{}, []byte{}})
	return io.MultiReader(bytes.NewReader(creationEntry), newDatastoreContentReader)
}

func CreateNewDatastoreReaderFromBytes(newDatastoreContentBytes []byte, creationTimestamp int64) io.Reader {
	return CreateNewDatastoreReader(bytes.NewReader(newDatastoreContentBytes), creationTimestamp)
}

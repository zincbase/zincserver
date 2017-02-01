package main

import (
	"io"
)

type EntryStreamIteratorFunc func() (*EntryStreamIteratorResult, error)

func NewEntryStreamIterator(source io.ReaderAt, startOffset int64, endOffset int64) EntryStreamIteratorFunc {
	// If the end offset is zero
	if endOffset == 0 {
		// Return an iterator that immediately completes
		return func() (*EntryStreamIteratorResult, error) {
			return nil, nil
		}
	}

	// Set the read offset to the start offset
	var readOffset int64 = startOffset

	// Return an iterator function
	return func() (*EntryStreamIteratorResult, error) {
		// If the current read offset equals the end offset
		if readOffset == endOffset {
			// Return an empty result with no error
			return nil, nil
		// Otherwise if the primary header end offset is greater than the streams's end offset
		} else if readOffset + PrimaryHeaderSize > endOffset {
			// Return an empty result with an unexpected end of stream error
			return nil, io.ErrUnexpectedEOF
		}

		// Parse the primary header
		primaryHeaderBytes := make([]byte, PrimaryHeaderSize)
		_, err := source.ReadAt(primaryHeaderBytes, readOffset)

		// If an error occurred when parsing the header
		if err != nil {
			// If the error was an EOF error
			if err == io.EOF {
				// Return an empty result with an unexpected end of stream error
				return nil, io.ErrUnexpectedEOF
			} else {
				// Otherwise return an empty result with the error
				return nil, err
			}
		}

		// Deserialize the primary header
		primaryHeader := DeserializePrimaryHeader(primaryHeaderBytes)

		// Create the iterator result object
		iteratorResult := &EntryStreamIteratorResult{
			source:             source,
			Offset:             readOffset,
			Size:               primaryHeader.TotalSize,
			PrimaryHeaderBytes: primaryHeaderBytes,
			PrimaryHeader:      primaryHeader,
		}

		// If the expected end offset of the entry is greater than the end offset of the stream
		if iteratorResult.EndOffset() > endOffset {
			// Return an empty result with an unexpected eof error
			return nil, io.ErrUnexpectedEOF
		}

		// Add the entry size to the read offset
		readOffset += iteratorResult.Size

		// Return the iterator result object, and no error
		return iteratorResult, nil
	}
}

// The entry stream iterator result object
type EntryStreamIteratorResult struct {
	// The source stream
	source             io.ReaderAt
	// The entry's offset relative to the stream
	Offset             int64
	// The entry's size
	Size               int64
	// The primary header
	PrimaryHeader      *EntryPrimaryHeader
	// The serialized primary header
	PrimaryHeaderBytes []byte
}

// Read the secondary header's bytes
func (this *EntryStreamIteratorResult) ReadSecondaryHeaderBytes() (secondaryHeaderBytes []byte, err error) {
	secondaryHeaderBytes = make([]byte, this.SecondaryHeaderSize())
	_, err = this.source.ReadAt(secondaryHeaderBytes, this.SecondaryHeaderOffset())
	return
}

// Read the key as a byte slice
func (this *EntryStreamIteratorResult) ReadKey() (key []byte, err error) {
	key = make([]byte, this.KeySize())
	_, err = this.source.ReadAt(key, this.KeyOffset())
	return
}

// Create a reader stream for the key
func (this *EntryStreamIteratorResult) CreateKeyReader() io.Reader {
	return NewRangeReader(this.source, this.KeyOffset(), this.ValueOffset())
}

// Read the value as a byte slice
func (this *EntryStreamIteratorResult) ReadValue() (value []byte, err error) {
	value = make([]byte, this.ValueSize())
	_, err = this.source.ReadAt(value, this.ValueOffset())
	return
}

// Create a reader stream for the value
func (this *EntryStreamIteratorResult) CreateValueReader() io.Reader {
	return NewRangeReader(this.source, this.ValueOffset(), this.EndOffset())
}

// Read the key and value as byte slices
func (this *EntryStreamIteratorResult) ReadKeyAndValue() (key []byte, value []byte, err error) {
	keyAndValue := make([]byte, this.KeySize()+this.ValueSize())
	key = keyAndValue[0:this.KeySize()]
	value = keyAndValue[this.KeySize():]
	_, err = this.source.ReadAt(keyAndValue, this.KeyOffset())
	return
}

// Create a reader for the payload (everything past the primary header)
func (this *EntryStreamIteratorResult) CreatePayloadReader() io.Reader {
	return NewRangeReader(this.source, this.SecondaryHeaderOffset(), this.EndOffset())
}

// Get the stream offset of the secondary header
func (this *EntryStreamIteratorResult) SecondaryHeaderOffset() int64 {
	return this.Offset + int64(PrimaryHeaderSize)
}

// Get the size of the secondary header
func (this *EntryStreamIteratorResult) SecondaryHeaderSize() int64 {
	return int64(this.PrimaryHeader.SecondaryHeaderSize)
}

// Get the stream offset of the key
func (this *EntryStreamIteratorResult) KeyOffset() int64 {
	return this.SecondaryHeaderOffset() + int64(this.PrimaryHeader.SecondaryHeaderSize)
}

// Get the size of the key
func (this *EntryStreamIteratorResult) KeySize() int64 {
	return int64(this.PrimaryHeader.KeySize)
}

// Get the stream offset of the value
func (this *EntryStreamIteratorResult) ValueOffset() int64 {
	return this.KeyOffset() + this.KeySize()
}

// Get the size of the value
func (this *EntryStreamIteratorResult) ValueSize() int64 {
	return this.EndOffset() - this.ValueOffset()
}

// Get the offset of the end of the entry (which is also the offset to the beginning of the
// next entry, if exists)
func (this *EntryStreamIteratorResult) EndOffset() int64 {
	return this.Offset + this.Size
}

// Does this entry have a transaction end flag?
func (this *EntryStreamIteratorResult) HasTransactionEndFlag() bool {
	return this.PrimaryHeader.Flags&Flag_TransactionEnd == Flag_TransactionEnd
}

// Is this entry a creation event?
func (this *EntryStreamIteratorResult) IsCreationEvent() bool {
	return this.PrimaryHeader.Flags&Flag_CreationEvent == Flag_CreationEvent
}

// Verify the primary header's checksum
func (this *EntryStreamIteratorResult) VerifyPrimaryHeaderChecksum() bool {
	return VerifyPrimaryHeaderChecksum(this.PrimaryHeaderBytes)
}

// Verify the payload's checksum
func (this *EntryStreamIteratorResult) VerifyPayloadChecksum() bool {
	return VerifyPayloadChecksum(this.PrimaryHeaderBytes, this.CreatePayloadReader())
}

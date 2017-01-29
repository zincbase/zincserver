package main

import (
	"io"
)

type EntryStreamIteratorFunc func() (*EntryStreamIteratorResult, error)

func NewEntryStreamIterator(source io.ReaderAt, startOffset int64, endOffset int64, checkTransactionEndFlag bool) EntryStreamIteratorFunc {
	if endOffset == 0 {
		return func() (*EntryStreamIteratorResult, error) {
			return nil, nil
		}
	}

	var readOffset int64 = startOffset
	var lastEntryHadTransactionEndFlag bool

	return func() (*EntryStreamIteratorResult, error) {
		if readOffset == endOffset {
			if checkTransactionEndFlag && !lastEntryHadTransactionEndFlag {
				return nil, io.ErrUnexpectedEOF
			} else {
				return nil, nil
			}
		} else if readOffset > endOffset {
			return nil, io.ErrUnexpectedEOF
		}

		headerBytes := make([]byte, PrimaryHeaderSize)
		_, err := source.ReadAt(headerBytes, readOffset)

		if err != nil {
			if err == io.EOF {
				return nil, io.ErrUnexpectedEOF
			} else {
				return nil, err
			}
		}

		header := ReadPrimaryHeader(headerBytes)

		iteratorResult := &EntryStreamIteratorResult{
			source:        source,
			Offset:        readOffset,
			Size:          header.TotalSize,
			PrimaryHeader: header,
		}

		if checkTransactionEndFlag {
			lastEntryHadTransactionEndFlag = iteratorResult.HasTransactionEndFlag()
		}

		readOffset += iteratorResult.Size
		return iteratorResult, nil
	}
}

func NewEntryRangeListIterator(source io.ReaderAt, ranges []Range, checkTransactionEndFlag bool) EntryStreamIteratorFunc {
	if len(ranges) == 0 {
		return func() (*EntryStreamIteratorResult, error) {
			return nil, nil
		}
	}

	var currentRangeIndex = 0
	var lastEntryHadTransactionEndFlag bool

	return func() (*EntryStreamIteratorResult, error) {
		if currentRangeIndex == len(ranges) {
			if checkTransactionEndFlag && !lastEntryHadTransactionEndFlag {
				return nil, io.ErrUnexpectedEOF
			} else {
				return nil, nil
			}
		}

		readOffset := ranges[currentRangeIndex].StartOffset
		headerBytes := make([]byte, PrimaryHeaderSize)
		_, err := source.ReadAt(headerBytes, readOffset)

		if err != nil {
			if err == io.EOF {
				return nil, io.ErrUnexpectedEOF
			} else {
				return nil, err
			}
		}

		header := ReadPrimaryHeader(headerBytes)

		iteratorResult := &EntryStreamIteratorResult{
			source:        source,
			Offset:        readOffset,
			Size:          header.TotalSize,
			PrimaryHeader: header,
		}

		if checkTransactionEndFlag {
			lastEntryHadTransactionEndFlag = iteratorResult.HasTransactionEndFlag()
		}

		currentRangeIndex++
		return iteratorResult, nil
	}
}

type EntryStreamIteratorResult struct {
	source        io.ReaderAt
	Offset        int64
	Size          int64
	PrimaryHeader *EntryPrimaryHeader
}

func (this *EntryStreamIteratorResult) ReadKey() (key []byte, err error) {
	key = make([]byte, this.KeySize())
	_, err = this.source.ReadAt(key, this.KeyOffset())
	return
}

func (this *EntryStreamIteratorResult) ReadValue() (value []byte, err error) {
	value = make([]byte, this.ValueSize())
	_, err = this.source.ReadAt(value, this.ValueOffset())
	return
}

func (this *EntryStreamIteratorResult) ReadKeyAndValue() (key []byte, value []byte, err error) {
	keyAndValue := make([]byte, this.KeySize()+this.ValueSize())
	key = keyAndValue[0:this.KeySize()]
	value = keyAndValue[this.KeySize():]
	_, err = this.source.ReadAt(keyAndValue, this.KeyOffset())
	return
}

func (this *EntryStreamIteratorResult) CreateKeyReader() io.Reader {
	return NewRangeReader(this.source, this.KeyOffset(), this.ValueOffset())
}

func (this *EntryStreamIteratorResult) CreateValueReader() io.Reader {
	return NewRangeReader(this.source, this.ValueOffset(), this.EndOffset())
}

func (this *EntryStreamIteratorResult) KeyOffset() int64 {
	return this.Offset + int64(PrimaryHeaderSize) + int64(this.PrimaryHeader.SecondaryHeaderSize)
}

func (this *EntryStreamIteratorResult) KeySize() int64 {
	return int64(this.PrimaryHeader.KeySize)
}

func (this *EntryStreamIteratorResult) ValueOffset() int64 {
	return this.KeyOffset() + this.KeySize()
}

func (this *EntryStreamIteratorResult) ValueSize() int64 {
	return this.EndOffset() - this.ValueOffset()
}

func (this *EntryStreamIteratorResult) EndOffset() int64 {
	return this.Offset + this.Size
}

func (this *EntryStreamIteratorResult) HasTransactionEndFlag() bool {
	return this.PrimaryHeader.Flags&Flag_TransactionEnd == Flag_TransactionEnd
}

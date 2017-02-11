package main

import (
	"io"
)

func CloneSlice(buffer []byte) (result []byte) {
	result = make([]byte, len(buffer))
	copy(result, buffer)
	return
}

func ReadEntireStream(reader io.Reader) (result []byte, err error) {
	memoryWriter := NewMemoryWriter()
	_, err = memoryWriter.CopyFromReader(reader)
	if err != nil {
		return
	}
	result = memoryWriter.WrittenData()
	return
}

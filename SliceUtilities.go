package main

import (
	"io"
)

func CloneSlice(buffer []byte) (result []byte) {
	result = make([]byte, len(buffer))
	copy(result, buffer)
	return
}

func ReadCompleteStream(reader io.Reader) (result []byte, err error) {
	memoryWriter := NewMemoryWriter()
	_, err = io.Copy(memoryWriter, reader)
	if err != nil {
		return
	}
	result = memoryWriter.WrittenData()
	return
}

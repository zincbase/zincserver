package main

import (
	"io"
)

func ConcatBufferList(buffers [][]byte) (result []byte) {
	totalLength := 0

	for _, buffer := range buffers {
		totalLength += len(buffer)
	}

	result = make([]byte, totalLength)

	offset := 0
	for _, buffer := range buffers {
		copy(result[offset:], buffer)
		offset += len(buffer)
	}

	return
}

func ConcatBuffers(buffers ...[]byte) (result []byte) {
	return ConcatBufferList(buffers)
}

func CloneBuffer(buffer []byte) (result []byte) {
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

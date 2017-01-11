package main

import (
//"io"
)

type MemoryWriter struct {
	result []byte
}

func (this *MemoryWriter) Write(p []byte) (n int, err error) {
	this.result = append(this.result, p...)
	return len(p), nil
}

func (this *MemoryWriter) WrittenData() []byte {
	return this.result
}

func NewMemoryWriter() *MemoryWriter {
	return &MemoryWriter{
		result: []byte{},
	}
}

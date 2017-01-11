package main

import "io"

type EmptyReader struct {
}

func (_ EmptyReader) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (_ EmptyReader) ReadAt(p []byte, offset int64) (n int, err error) {
	return 0, io.EOF
}
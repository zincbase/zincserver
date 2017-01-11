package main

import (
	"reflect"
	"testing"
)

func Test_BufferUtilities(test *testing.T) {
	test.Run("Concat buffer list", func(t *testing.T) {
		buf1 := []byte{1, 2, 3, 4}
		buf2 := []byte{5, 6, 7}
		buf3 := []byte{8, 9}

		result := ConcatBufferList([][]byte{buf1, buf2, buf3})

		if !reflect.DeepEqual(result, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
			test.Error("Data mismatch.")
		}		
	})

	test.Run("Clone buffer", func(t *testing.T) {
		buf1 := []byte{1, 2, 3, 4}
		result := CloneBuffer(buf1)

		if !reflect.DeepEqual(result, buf1) {
			test.Error("Data mismatch.")
		}		
	})			
}

package main

import (
	"testing"
	"reflect"
	"bytes"
)

func Test_RangeListReader(test *testing.T) {
	data := []byte{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17}

	rangeListReader := NewRangeListReader(bytes.NewReader(data), []Range{Range{1,4}, Range{7, 8}, Range{12, 16}})

	buf := make([]byte, 2)
	n, err := rangeListReader.Read(buf)

	if err != nil || n != 2 || !reflect.DeepEqual(buf, []byte{1,2}) {
		test.Error("Reading bytes 1-2 failed")
	}

	buf = make([]byte, 5)
	n, err = rangeListReader.Read(buf)

	if err != nil || n != 5 || !reflect.DeepEqual(buf, []byte{3,7,12,13,14}) {
		test.Error("Reading bytes 2-7 failed")
	}
}
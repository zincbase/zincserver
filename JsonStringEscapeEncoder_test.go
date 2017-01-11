package main

import (
	//"log"
	"encoding/json"
	"reflect"
	"testing"
)

func Test_JsonStringEscapeEncoder(t *testing.T) {
	test := func(t *testing.T, maxCodePoint int) {
		randomUtf8 := RandomUtf8String(100000, maxCodePoint)
		memWriter := NewMemoryWriter()
		encoder := NewJsonStringEscapeEncoder(memWriter)
		encoder.Write([]byte(randomUtf8))

		encodedString := string(memWriter.WrittenData())
		var decodedString string
		err := json.Unmarshal([]byte(`"`+encodedString+`"`), &decodedString)

		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(decodedString, randomUtf8) {
			t.Error("Decoded string doesn't match source")
		}
	}

	t.Run("Codepoints up to 300", func(t *testing.T) {
		test(t, 300)
	})

	t.Run("Codepoints up to 10000", func(t *testing.T) {
		test(t, 10000)
	})

	t.Run("Codepoints up to to the highest unicode range", func(t *testing.T) {
		test(t, -1)
	})
}

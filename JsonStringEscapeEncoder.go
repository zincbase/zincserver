package main

import (
	"encoding/hex"
	"io"
	"strings"
)

var jsonCharEscapeLookupTable [256]string

func init() {
	for c := 0; c < 256; c++ {
		switch c {
		case 0, 1, 2, 3, 4, 5, 6, 7, 11, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 127:
			jsonCharEscapeLookupTable[c] = "\\u" + strings.ToUpper(hex.EncodeToString([]byte{0, byte(c)}))
		case '"':
			jsonCharEscapeLookupTable[c] = `\"`
		case '\\':
			jsonCharEscapeLookupTable[c] = `\\`
		case '\b':
			jsonCharEscapeLookupTable[c] = `\b`
		case '\f':
			jsonCharEscapeLookupTable[c] = `\f`
		case '\n':
			jsonCharEscapeLookupTable[c] = `\n`
		case '\r':
			jsonCharEscapeLookupTable[c] = `\r`
		case '\t':
			jsonCharEscapeLookupTable[c] = `\t`
		default:
			jsonCharEscapeLookupTable[c] = string([]byte{byte(c)})
		}
	}
}

type JsonStringEscapeEncoder struct {
	destWriter io.Writer
}

func (this *JsonStringEscapeEncoder) Write(p []byte) (int, error) {
	// Notes:
	// * this encoder does not encode the solidus ('/') character as it is most likely not necessary
	// (Javascript's JSON encoder does not encode it as well).
	// * For more flexibility, the encoder doesn't add the preceding and trailing quotation marks ('"').
	result := []byte{}

	for _, c := range p {
		if c >= 32 && c != '"' && c != '\\' && c != 127 {
			result = append(result, c)
		} else {
			result = append(result, jsonCharEscapeLookupTable[c]...)
		}
	}

	_, err := this.destWriter.Write(result)
	return len(p), err
}

func NewJsonStringEscapeEncoder(w io.Writer) io.Writer {
	return &JsonStringEscapeEncoder{
		destWriter: w,
	}
}

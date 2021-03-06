package main

import (
	"math/rand"
	//"unsafe"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"hash/crc32"
	"io"
	"time"
	"log"
)

var CastagnoliCRC32Table *crc32.Table
var globalRand *rand.Rand

func init() {
	CastagnoliCRC32Table = crc32.MakeTable(crc32.Castagnoli)
	globalRand = rand.New(rand.NewSource(int64(time.Now().UnixNano())))
}

func Log(v ...interface{}) {
	log.Println(v...)
}

func Logf(format string, v ...interface{}) {
	log.Printf(format, v...)
}


func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func MaxInt(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func MaxInt64(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

func MaxFloat64(a, b float64) float64 {
	if a < b {
		return b
	}
	return a
}

func RandomBytes(length int) (result []byte) {
	result = make([]byte, length)
	globalRand.Read(result)
	return result
}

func RandomWordString(length int) string {
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	resultBytes := []byte{}

	for i := 0; i < length; i++ {
		resultBytes = append(resultBytes, letters[globalRand.Intn(len(letters))])
	}

	return string(resultBytes)
}

func RandomIntInRange(min int, max int) int {
	return min + rand.Intn(max-min)
}

func RandomInt63InRange(min int64, max int64) int64 {
	return min + rand.Int63n(max-min)
}

func RandomUtf8String(length int, maxCodePoint int) string {
	if maxCodePoint == -1 {
		maxCodePoint = 1114112
	}

	resultBuffer := bytes.Buffer{}

	for i := 0; i < length; i++ {
		resultBuffer.WriteRune(rune(globalRand.Intn(maxCodePoint)))
	}

	return resultBuffer.String()
}

func Sleep(durationMilliseconds float64) {
	time.Sleep(time.Duration(int64(durationMilliseconds * float64(time.Millisecond))))
}

func SHA1ToHex(data []byte) string {
	hash := sha1.Sum(data)
	return hex.EncodeToString(hash[:])
}

func SHA1ToString(data []byte) string {
	hash := sha1.Sum(data)
	return string(hash[:])
}

func CRC32C(data []byte) uint32 {
	return crc32.Checksum(data, CastagnoliCRC32Table)
}

func CRC32COfReader(reader io.Reader) (crc uint32, err error) {
	crc32c := crc32.New(CastagnoliCRC32Table)
	_, err = io.Copy(crc32c, reader)

	if err != nil {
		return
	}

	crc = crc32c.Sum32()

	return
}

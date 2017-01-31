package main

import (
	"math/rand"
	//"unsafe"
	"bytes"
)

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

func RandomWordString(length int) string {

	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	resultBytes := []byte{}

	random := rand.New(rand.NewSource(MonoUnixTimeNano()))
	for i := 0; i < length; i++ {
		resultBytes = append(resultBytes, letters[random.Intn(len(letters))])
	}

	return string(resultBytes)
}

func RandomUtf8String(length int, maxCodePoint int) string {
	if maxCodePoint == -1 {
		maxCodePoint = 1114112
	}

	resultBuffer := bytes.Buffer{}

	random := rand.New(rand.NewSource(MonoUnixTimeNano()))
	for i := 0; i < length; i++ {
		resultBuffer.WriteRune(rune(random.Intn(maxCodePoint)))
	}

	return resultBuffer.String()
}

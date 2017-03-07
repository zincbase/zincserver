package main

import (
	"testing"
)

func Benchmark_HeaderSerializer(bench *testing.B) {
	bench.Run("Serialization (architecture dependant implementation)", func(bench *testing.B) {
		testHeader := &EntryHeader{
			TotalSize:        33452341,
			HeaderVersion:    4321,
			KeySize:          3945,
			KeyFormat:        54,
			ValueFormat:      23,
			EncryptionMethod: 34,
			Flags:            41,
			UpdateTime: 345343452123,
			CommitTime: 345343452345,
		}

		serializedHeader := make([]byte, HeaderSize)

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			SerializeHeader(testHeader, serializedHeader)
		}
	})

	bench.Run("Deserialization (architecture dependant implementation)", func(bench *testing.B) {
		testHeader := &EntryHeader{
			TotalSize:        33452341,
			HeaderVersion:    4321,
			KeySize:          3945,
			KeyFormat:        54,
			ValueFormat:      23,
			EncryptionMethod: 34,
			Flags:            41,
			UpdateTime: 345343452123,
			CommitTime: 345343452345,
		}

		serializedHeader := make([]byte, HeaderSize)
		SerializeHeader(testHeader, serializedHeader)

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			_ = DeserializeHeader(serializedHeader)
		}
	})

	bench.Run("Serialization (slow implementation for BE architectures)", func(bench *testing.B) {
		testHeader := &EntryHeader{
			TotalSize:        33452341,
			HeaderVersion:    4321,
			KeySize:          3945,
			KeyFormat:        54,
			ValueFormat:      23,
			EncryptionMethod: 34,
			Flags:            41,
			UpdateTime: 345343452123,
			CommitTime: 345343452345,
		}

		serializedHeader := make([]byte, HeaderSize)

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			SerializeHeader_Slow(testHeader, serializedHeader)
		}
	})

	bench.Run("Deserialization (slow implementation for BE architectures)", func(bench *testing.B) {
		testHeader := &EntryHeader{
			TotalSize:        33452341,
			HeaderVersion:    4321,
			KeySize:          3945,
			KeyFormat:        54,
			ValueFormat:      23,
			EncryptionMethod: 34,
			Flags:            41,
			UpdateTime: 345343452123,
			CommitTime: 345343452345,
		}

		serializedHeader := make([]byte, HeaderSize)
		SerializeHeader_Slow(testHeader, serializedHeader)

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			_ = DeserializeHeader_Slow(serializedHeader)
		}
	})
}

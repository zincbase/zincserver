package main

import (
	"testing"
)

func Benchmark_HeaderSerializer(bench *testing.B) {
	bench.Run("Serialization (architecture dependant implementation)", func(bench *testing.B) {
		testHeader := &EntryPrimaryHeader{
			TotalSize:           33452341,
			UpdateTime:          345343452123,
			CommitTime:          345343452345,
			KeySize:             3945,
			KeyFormat:           54,
			ValueFormat:         23,
			EncryptionMethod:    34,
			Flags:               41,
			SecondaryHeaderSize: 5436,
		}
		serializedHeader := make([]byte, PrimaryHeaderSize)

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			SerializePrimaryHeader(serializedHeader, testHeader)
		}
	})

	bench.Run("Deserialization (architecture dependant implementation)", func(bench *testing.B) {
		testHeader := &EntryPrimaryHeader{
			TotalSize:           33452341,
			UpdateTime:          345343452123,
			CommitTime:          345343452345,
			KeySize:             3945,
			KeyFormat:           54,
			ValueFormat:         23,
			EncryptionMethod:    34,
			Flags:               41,
			SecondaryHeaderSize: 5436,
		}
		serializedHeader := make([]byte, PrimaryHeaderSize)
		SerializePrimaryHeader(serializedHeader, testHeader)

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			_ = DeserializePrimaryHeader(serializedHeader)
		}
	})

	bench.Run("Serialization (slow implementation for BE architectures)", func(bench *testing.B) {
		testHeader := &EntryPrimaryHeader{
			TotalSize:           33452341,
			UpdateTime:          345343452123,
			CommitTime:          345343452345,
			KeySize:             3945,
			KeyFormat:           54,
			ValueFormat:         23,
			EncryptionMethod:    34,
			Flags:               41,
			SecondaryHeaderSize: 5436,
		}
		serializedHeader := make([]byte, PrimaryHeaderSize)

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			SerializePrimaryHeader_Slow(serializedHeader, testHeader)
		}
	})

	bench.Run("Deserialization (slow implementation for BE architectures)", func(bench *testing.B) {
		testHeader := &EntryPrimaryHeader{
			TotalSize:           33452341,
			UpdateTime:          345343452123,
			CommitTime:          345343452345,
			KeySize:             3945,
			KeyFormat:           54,
			ValueFormat:         23,
			EncryptionMethod:    34,
			Flags:               41,
			SecondaryHeaderSize: 5436,
		}
		serializedHeader := make([]byte, PrimaryHeaderSize)
		SerializePrimaryHeader_Slow(serializedHeader, testHeader)

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			_ = DeserializePrimaryHeader_Slow(serializedHeader)
		}
	})	
}

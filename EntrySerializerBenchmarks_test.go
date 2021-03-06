package main

import (
	"testing"
)

func Benchmark_EntrySerializer(bench *testing.B) {
	bench.Run("Serialization", func(bench *testing.B) {
		testEntry := Entry{
			Header: &EntryHeader{
				UpdateTime:      2345345222523423132,
				CommitTime:      2345345222523423423,
				HeaderChecksum:  3316190138, // Bogus checksum for testing only
				PayloadChecksum: 2042592394, // Bogus checksum for testing only
			},
			Key:   []byte("Test Key"),
			Value: []byte("Test Value"),
		}

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			_ = SerializeEntry(&testEntry)
		}
	})

	bench.Run("Deserialization", func(bench *testing.B) {
		testEntry := Entry{
			Header: &EntryHeader{
				UpdateTime:            2345345222523423132,
				CommitTime:            2345345222523423423,
				HeaderChecksum: 3316190138, // Bogus checksum for testing only
				PayloadChecksum:       2042592394, // Bogus checksum for testing only
			},
			Key:                  []byte("Test Key"),
			Value:                []byte("Test Value"),
		}

		serializedTestEntry := SerializeEntry(&testEntry)

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			_ = DeserializeEntry(serializedTestEntry)
		}
	})
}

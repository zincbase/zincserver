package main

import (
	"testing"
)

func Benchmark_EntrySerializer(bench *testing.B) {
	bench.Run("Serialization", func(bench *testing.B) {
		testEntry := Entry{
			PrimaryHeader: &EntryPrimaryHeader{
				UpdateTime: 2345345222523423132,
				CommitTime: 2345345222523423423,
			},
			SecondaryHeaderBytes: []byte("Secondary Header"),
			Key:                  []byte("Test Key"),
			Value:                []byte("Test Value"),
		}

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			_ = SerializeEntry(&testEntry)
		}
	})

	bench.Run("Deserialization", func(bench *testing.B) {
		testEntry := Entry{
			PrimaryHeader: &EntryPrimaryHeader{
				UpdateTime: 2345345222523423132,
				CommitTime: 2345345222523423423,
			},
			SecondaryHeaderBytes: []byte("Secondary Header"),
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

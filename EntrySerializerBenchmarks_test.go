package main

import (
	"testing"
)

func Benchmark_EntrySerializer(bench *testing.B) {
	bench.Run("Serialization", func(bench *testing.B) {
		testEntry := Entry{
			Key:   []byte("Test Key"),
			Value: []byte("Test Value"),
			PrimaryHeader: &EntryPrimaryHeader{
				CommitTime: 2345345222523423423,
			},
		}

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			_ = SerializeEntry(&testEntry)
		}
	})

	bench.Run("Deserialization", func(bench *testing.B) {
		testEntry := Entry{
			Key:   []byte("Test Key"),
			Value: []byte("Test Value"),
			PrimaryHeader: &EntryPrimaryHeader{
				CommitTime: 2345345222523423423,
			},
		}

		serializedTestEntry := SerializeEntry(&testEntry)

		bench.ResetTimer()
		for i := 0; i < bench.N; i++ {
			_ = DeserializeEntry(serializedTestEntry)
		}
	})

}

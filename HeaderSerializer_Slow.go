package main

import (
	"encoding/binary"
)

// Slow implementations for native big endian platforms

////////////////////////////////////////////////////////////////////////////////
// Serialization
////////////////////////////////////////////////////////////////////////////////
func SerializeHeader_Slow(header *EntryHeader, targetSlice []byte) {
	binary.LittleEndian.PutUint64(targetSlice[0:8], uint64(header.TotalSize))
	binary.LittleEndian.PutUint16(targetSlice[8:10], header.HeaderVersion)
	binary.LittleEndian.PutUint16(targetSlice[10:12], header.KeySize)
	targetSlice[12] = header.KeyFormat
	targetSlice[13] = header.ValueFormat
	targetSlice[14] = header.EncryptionMethod
	targetSlice[15] = header.Flags
	binary.LittleEndian.PutUint64(targetSlice[16:24], uint64(header.UpdateTime))
	binary.LittleEndian.PutUint64(targetSlice[24:32], uint64(header.CommitTime))
	binary.LittleEndian.PutUint32(targetSlice[32:36], header.HeaderChecksum)
	binary.LittleEndian.PutUint32(targetSlice[36:40], header.PayloadChecksum)
}

////////////////////////////////////////////////////////////////////////////////
// Deserialization
////////////////////////////////////////////////////////////////////////////////
func DeserializeHeader_Slow(headerBytes []byte) *EntryHeader {
	return &EntryHeader{
		TotalSize:        int64(binary.LittleEndian.Uint64(headerBytes[0:8])),
		HeaderVersion:    binary.LittleEndian.Uint16(headerBytes[8:10]),
		KeySize:          binary.LittleEndian.Uint16(headerBytes[10:12]),
		KeyFormat:        headerBytes[12],
		ValueFormat:      headerBytes[13],
		EncryptionMethod: headerBytes[14],
		Flags:            headerBytes[15],
		UpdateTime:       int64(binary.LittleEndian.Uint64(headerBytes[16:24])),
		CommitTime:       int64(binary.LittleEndian.Uint64(headerBytes[24:32])),
		HeaderChecksum:   binary.LittleEndian.Uint32(headerBytes[32:36]),
		PayloadChecksum:  binary.LittleEndian.Uint32(headerBytes[36:40]),
	}
}

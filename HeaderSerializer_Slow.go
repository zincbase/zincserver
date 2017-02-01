package main

import (
	"encoding/binary"
)

// Slow implementations for native big endian platforms

////////////////////////////////////////////////////////////////////////////////
// Serialization
////////////////////////////////////////////////////////////////////////////////
func SerializePrimaryHeader_Slow(targetSlice []byte, header *EntryPrimaryHeader) {
	binary.LittleEndian.PutUint64(targetSlice[0:8], uint64(header.TotalSize))
	binary.LittleEndian.PutUint64(targetSlice[8:16], uint64(header.UpdateTime))
	binary.LittleEndian.PutUint64(targetSlice[16:24], uint64(header.CommitTime))
	binary.LittleEndian.PutUint16(targetSlice[24:26], header.KeySize)
	targetSlice[26] = header.KeyFormat
	targetSlice[27] = header.ValueFormat
	targetSlice[28] = header.EncryptionMethod
	targetSlice[29] = header.Flags
	binary.LittleEndian.PutUint16(targetSlice[30:32], header.SecondaryHeaderSize)
	binary.LittleEndian.PutUint32(targetSlice[32:36], header.PrimaryHeaderChecksum)
	binary.LittleEndian.PutUint32(targetSlice[36:40], header.PayloadChecksum)
}

////////////////////////////////////////////////////////////////////////////////
// Deserialization
////////////////////////////////////////////////////////////////////////////////
func DeserializePrimaryHeader_Slow(primaryHeaderBytes []byte) *EntryPrimaryHeader {
	return &EntryPrimaryHeader{
		TotalSize:             int64(binary.LittleEndian.Uint64(primaryHeaderBytes[0:8])),
		UpdateTime:            int64(binary.LittleEndian.Uint64(primaryHeaderBytes[8:16])),
		CommitTime:            int64(binary.LittleEndian.Uint64(primaryHeaderBytes[16:24])),
		KeySize:               binary.LittleEndian.Uint16(primaryHeaderBytes[24:26]),
		KeyFormat:             primaryHeaderBytes[26],
		ValueFormat:           primaryHeaderBytes[27],
		EncryptionMethod:      primaryHeaderBytes[28],
		Flags:                 primaryHeaderBytes[29],
		SecondaryHeaderSize:   binary.LittleEndian.Uint16(primaryHeaderBytes[30:32]),
		PrimaryHeaderChecksum: binary.LittleEndian.Uint32(primaryHeaderBytes[32:36]),
		PayloadChecksum:       binary.LittleEndian.Uint32(primaryHeaderBytes[36:40]),
	}
}

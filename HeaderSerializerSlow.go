package main

// Slow implementations for native big endian platforms

////////////////////////////////////////////////////////////////////////////////
// Serialization
////////////////////////////////////////////////////////////////////////////////
func SerializePrimaryHeader_Slow(targetSlice []byte, header *EntryPrimaryHeader) {
	WriteInt64LE_Slow(targetSlice[0:8], header.TotalSize)
	WriteInt64LE_Slow(targetSlice[8:16], header.UpdateTime)
	WriteInt64LE_Slow(targetSlice[16:24], header.CommitTime)
	WriteUint16LE_Slow(targetSlice[24:26], header.KeySize)
	targetSlice[26] = header.KeyFormat
	targetSlice[27] = header.ValueFormat
	targetSlice[28] = header.EncryptionMethod
	targetSlice[29] = header.Flags
	WriteUint16LE_Slow(targetSlice[30:32], header.SecondaryHeaderSize)
}

////////////////////////////////////////////////////////////////////////////////
// Deserialization
////////////////////////////////////////////////////////////////////////////////
func DeserializePrimaryHeader_Slow(primaryHeaderBytes []byte) *EntryPrimaryHeader {
	return &EntryPrimaryHeader {
		TotalSize: int64(ReadInt64LE_Slow(primaryHeaderBytes[0:8])),
		UpdateTime: int64(ReadInt64LE_Slow(primaryHeaderBytes[8:16])),
		CommitTime: int64(ReadInt64LE_Slow(primaryHeaderBytes[16:24])),
		KeySize: ReadUint16LE_Slow(primaryHeaderBytes[24:26]),
		KeyFormat: primaryHeaderBytes[26],
		ValueFormat: primaryHeaderBytes[27],
		EncryptionMethod: primaryHeaderBytes[28],
		Flags: primaryHeaderBytes[29],
		SecondaryHeaderSize: ReadUint16LE_Slow(primaryHeaderBytes[30:32]),
	}
}

////////////////////////////////////////////////////////////////////////////////
// Slow binary write functions
////////////////////////////////////////////////////////////////////////////////
func WriteUint16LE_Slow(b []byte, v uint16) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
}

func WriteInt64LE_Slow(b []byte, v int64) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)	
}

////////////////////////////////////////////////////////////////////////////////
// Slow binary read functions
////////////////////////////////////////////////////////////////////////////////
func ReadUint16LE_Slow(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}

func ReadInt64LE_Slow(b []byte) int64 {
	return int64(b[0]) | int64(b[1])<<8 | int64(b[2])<<16 | int64(b[3])<<24 | int64(b[4])<<32 | int64(b[5])<<40 | int64(b[6])<<48 | int64(b[7])<<56
}
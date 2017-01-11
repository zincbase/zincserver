// +build mips64 ppc64

package main

import (
	"unsafe"
)

// Fallback implementation for big endian architectures

/////////////////////////////////////////////////////////////////////////////////////////////
// Read functions
/////////////////////////////////////////////////////////////////////////////////////////////
func ReadUint16LE(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}

func ReadUint48LE(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40
}

func ReadUint64LE(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func ReadFloat64LE(b []byte) float64 {
	uint64Value := ReadUint64LE(b)
	return *(*float64)(unsafe.Pointer(&uint64Value))
}

/////////////////////////////////////////////////////////////////////////////////////////////
// Write functions
/////////////////////////////////////////////////////////////////////////////////////////////
func WriteUint16LE(b []byte, v uint16) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
}

func WriteUint48LE(b []byte, v uint64) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
}

func WriteUint64LE(b []byte, v uint64) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)	
}

func WriteFloat64LE(b []byte, v float64) {
	var uint64Value uint64
	*(*float64)(unsafe.Pointer(&uint64Value)) = v
	WriteUint64LE(b, uint64Value)
}

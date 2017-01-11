// +build amd64 386 arm arm64 ppc64le mips64le

package main

import (
	"unsafe"
)

// Fast implementation for little endian architectures

/////////////////////////////////////////////////////////////////////////////////////////////
// Read functions
/////////////////////////////////////////////////////////////////////////////////////////////
func ReadUint16LE(b []byte) uint16 {
	return *(*uint16)(unsafe.Pointer(&b[0]))
}

func ReadUint48LE(b []byte) uint64 {
	return uint64(*(*uint32)(unsafe.Pointer(&b[0]))) | uint64(*(*uint16)(unsafe.Pointer(&b[4]))) << 32
}

func ReadUint64LE(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

func ReadFloat64LE(b []byte) float64 {
	return *(*float64)(unsafe.Pointer(&b[0]))
}

/////////////////////////////////////////////////////////////////////////////////////////////
// Write functions
/////////////////////////////////////////////////////////////////////////////////////////////
func WriteUint16LE(b []byte, v uint16) {
	*(*uint16)(unsafe.Pointer(&b[0])) = v
}

func WriteUint48LE(b []byte, v uint64) {
	*(*uint32)(unsafe.Pointer(&b[0])) = uint32(v & ((1 << 32) - 1))
	*(*uint16)(unsafe.Pointer(&b[4])) = uint16(v >> 32)
}

func WriteUint64LE(b []byte, v uint64) {
	*(*uint64)(unsafe.Pointer(&b[0])) = v
}

func WriteFloat64LE(b []byte, v float64) {
	*(*float64)(unsafe.Pointer(&b[0])) = v
}

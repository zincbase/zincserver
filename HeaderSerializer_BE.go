// +build mips64 ppc64

package main

// Slow implementations for big endian architectures.
//
// The actual implementations are in a separate file, to allow them to be tested 
// on LE architectures as well.

////////////////////////////////////////////////////////////////////////////////
// Serialization
////////////////////////////////////////////////////////////////////////////////
func SerializePrimaryHeader(targetSlice []byte, header *EntryPrimaryHeader) {
	SerializePrimaryHeader_Slow(targetSlice, header)
}

////////////////////////////////////////////////////////////////////////////////
// Deserialization
////////////////////////////////////////////////////////////////////////////////
func DeserializePrimaryHeader(primaryHeaderBytes []byte) *EntryPrimaryHeader {
	return DeserializePrimaryHeader_Slow(primaryHeaderBytes)
}
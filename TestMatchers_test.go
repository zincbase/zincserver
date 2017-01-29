package main

import (
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	. "github.com/onsi/gomega/types"
)

func EqualEntry(expected Entry) GomegaMatcher {
	return SatisfyAll(
		BeAssignableToTypeOf(Entry{nil, nil, []byte{}, []byte{}}),
		MatchAllFields(Fields{
			"PrimaryHeader":   Equal(expected.PrimaryHeader),
			"SecondaryHeaderBytes": Equal(expected.SecondaryHeaderBytes),
			"Key":             Equal(expected.Key),
			"Value":           Equal(expected.Value),
		}),
	)
}

func ExpectEntryArraysToBeEqual(entries1 []Entry, entries2 []Entry) bool {
	Expect(entries1).To(HaveLen(len(entries2)))

	for i := 0; i < len(entries1); i++ {
		Expect(entries1[i]).To(EqualEntry(entries2[i]))
	}

	return true
}

func EqualNumber(expected interface{}) GomegaMatcher {
	return BeNumerically("==", expected)
}

package main

import (
	. "github.com/onsi/gomega"
	//. "github.com/onsi/gomega/gstruct"
	. "github.com/onsi/gomega/types"
)

func ExpectEntriesToBeEqual(entry1 Entry, entry2 Entry) {
	Expect(entry1.Header).To(Equal(entry2.Header))
	Expect(entry1.Key).To(Equal(entry2.Key))
	Expect(entry1.Value).To(Equal(entry2.Value))
}

func ExpectEntryArraysToBeEqual(entries1 []Entry, entries2 []Entry) bool {
	Expect(entries1).To(HaveLen(len(entries2)))

	for i := 0; i < len(entries1); i++ {
		ExpectEntriesToBeEqual(entries1[i], entries2[i])
	}

	return true
}

func ExpectEntryArraysToBeEqual_IgnoreHead(entries1 []Entry, entries2 []Entry) bool {
	Expect(entries1).To(HaveLen(len(entries2)))

	for i := 0; i < len(entries1); i++ {
		if len(entries1[i].Key) == 0 && len(entries2[i].Key) == 0 {
			continue
		}

		ExpectEntriesToBeEqual(entries1[i], entries2[i])
	}

	return true
}

func ExpectEntriesToBeEquivalent(entry1 Entry, entry2 Entry) {
	header1 := entry1.Header
	header2 := entry2.Header

	Expect(header1.TotalSize).To(Equal(header2.TotalSize))
	Expect(header1.KeySize).To(Equal(header2.KeySize))
	Expect(header1.KeyFormat).To(Equal(header2.KeyFormat))
	Expect(header1.ValueFormat).To(Equal(header2.ValueFormat))
	Expect(header1.EncryptionMethod).To(Equal(header2.EncryptionMethod))

	Expect(entry1.Key).To(Equal(entry2.Key))
	Expect(entry1.Value).To(Equal(entry2.Value))
}

func ExpectEntriesToHaveCommitTimestamp(entries []Entry, commitTimestamp int64) {
	for i := 0; i < len(entries); i++ {
		Expect(entries[i].Header.CommitTime).To(Equal(commitTimestamp))
	}
}
func ExpectEntryArraysToBeEquivalent(entries1 []Entry, entries2 []Entry) {
	Expect(entries1).To(HaveLen(len(entries2)))

	for i := 0; i < len(entries1); i++ {
		if len(entries1[i].Key) == 0 && len(entries2[i].Key) == 0 {
			continue
		}

		ExpectEntriesToBeEquivalent(entries1[i], entries2[i])
	}
}

func ExpectEntryArraysToBeEquivalentWhenCompacted(entries1 []Entry, entries2 []Entry) {
	ExpectEntryArraysToBeEquivalent(CompactEntries(entries1), CompactEntries(entries2))
}


func EqualNumber(expected interface{}) GomegaMatcher {
	return BeNumerically("==", expected)
}

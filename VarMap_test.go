package main

import (
	"sort"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("VarMap", func() {
	It("Stores and gets values of various types", func() {
		varMap := NewEmptyVarMap()

		Expect(varMap.Count()).To(Equal(0))
		varMap.Set("Hello", "World")
		Expect(varMap.Count()).To(Equal(1))
		varMap.Set("Yo", true)
		Expect(varMap.Count()).To(Equal(2))
		varMap.Set("Hey ya", float64(1234))
		Expect(varMap.Count()).To(Equal(3))
		varMap.Set("Hi", float64(345345.453234))
		Expect(varMap.Count()).To(Equal(4))

		keys := varMap.Keys()
		sort.Strings(keys)
		Expect(keys).To(Equal([]string{"Hello", "Hey ya", "Hi", "Yo"}))

		result1, err := varMap.GetString("Hello")
		Expect(err).To(BeNil())
		Expect(result1).To(Equal("World"))

		result2, err := varMap.GetBool("Yo")
		Expect(err).To(BeNil())
		Expect(result2).To(Equal(true))

		result3, err := varMap.GetInt64("Hey ya")
		Expect(err).To(BeNil())
		Expect(result3).To(Equal(int64(1234)))

		result4, err := varMap.GetFloat64("Hi")
		Expect(err).To(BeNil())
		Expect(result4).To(Equal(float64(345345.453234)))
	})

	It("Checks for existence of keys", func() {
		varMap := NewEmptyVarMap()
		varMap.Set("Hello", "World")
		varMap.Set("Hi", float64(345345.453234))

		Expect(varMap.Has("Hello")).To(BeTrue())
		Expect(varMap.Has("Hi")).To(BeTrue())
		Expect(varMap.Has("Da")).To(BeFalse())
	})

	It("Appends JSON entries", func() {
		jsonEntries := []Entry{
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 1"), []byte(`"Key1"`), []byte(`"Hello world!"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 2"), []byte(`"Key2"`), []byte(`6543.21`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 3"), []byte(`"Key3"`), []byte(`true`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 4"), []byte(`"Key4"`), []byte(`false`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 5"), []byte(`"Key5"`), []byte(`null`)},
		}

		varMap := NewEmptyVarMap()
		varMap.Set("Key0", "Yup")
		Expect(varMap.Count()).To(Equal(1))

		err := varMap.AppendJsonEntries(jsonEntries)
		Expect(err).To(BeNil())

		Expect(varMap.Count()).To(Equal(6))

		keys := varMap.Keys()
		sort.Strings(keys)
		Expect(keys).To(Equal([]string{"Key0", "Key1", "Key2", "Key3", "Key4", "Key5"}))

		result1, err := varMap.GetString("Key1")
		Expect(err).To(BeNil())
		Expect(result1).To(Equal("Hello world!"))

		result2, err := varMap.GetFloat64("Key2")
		Expect(err).To(BeNil())
		Expect(result2).To(Equal(6543.21))

		result3, err := varMap.GetBool("Key3")
		Expect(err).To(BeNil())
		Expect(result3).To(Equal(true))

		result4, err := varMap.GetBool("Key4")
		Expect(err).To(BeNil())
		Expect(result4).To(Equal(false))

		result5, err := varMap.GetAny("Key5")
		Expect(err).To(BeNil())
		Expect(result5).To(BeNil())
	})

	It("Ignores non-JSON entries", func() {
		nonJsonEntries := []Entry{
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_Binary, Flags: Flag_TransactionEnd}, []byte("Secondary Header 1"), []byte(`"Key1"`), []byte(`"Hello world!"`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_JSON, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 2"), []byte(`"Key2"`), []byte(`1111.21`)},
			Entry{&EntryPrimaryHeader{KeyFormat: DataFormat_UTF8, ValueFormat: DataFormat_JSON, Flags: Flag_TransactionEnd}, []byte("Secondary Header 4"), []byte(`"Key3"`), []byte(`6543.21`)},
		}

		varMap := NewEmptyVarMap()

		varMap.AppendJsonEntries(nonJsonEntries);

		Expect(varMap.Count()).To(Equal(1))
		Expect(varMap.Has("Key2")).To(BeTrue())
	})

	It("Clones itself", func() {
		varMap := NewEmptyVarMap()

		varMap.Set("Hello", "World")
		varMap.Set("Yo", true)
		varMap.Set("Hey ya", float64(1234))
		varMap.Set("Hi", float64(345345.453234))

		varMapClone := varMap.Clone()

		Expect(varMapClone.Count()).To(Equal(4))

		result1, err := varMapClone.GetString("Hello")
		Expect(err).To(BeNil())
		Expect(result1).To(Equal("World"))

		result2, err := varMapClone.GetBool("Yo")
		Expect(err).To(BeNil())
		Expect(result2).To(Equal(true))

		result3, err := varMapClone.GetInt64("Hey ya")
		Expect(err).To(BeNil())
		Expect(result3).To(Equal(int64(1234)))

		result4, err := varMapClone.GetFloat64("Hi")
		Expect(err).To(BeNil())
		Expect(result4).To(Equal(float64(345345.453234)))

		varMap.Set("Hello", "Universe")

		result1, err = varMapClone.GetString("Hello")
		Expect(err).To(BeNil())
		Expect(result1).To(Equal("World"))
	})

	It("Deletes a key", func() {
		varMap := NewEmptyVarMap()

		varMap.Set("Hello", "World")
		varMap.Set("Yo", true)

		Expect(varMap.Count()).To(Equal(2))
		varMap.Delete("Yo")

		Expect(varMap.Count()).To(Equal(1))

		result1, err := varMap.GetString("Hello")
		Expect(err).To(BeNil())
		Expect(result1).To(Equal("World"))

		result2, err := varMap.GetBool("Yo")
		Expect(err).NotTo(BeNil())
		Expect(result2).To(Equal(false))
	})

	It("Clears", func() {
		varMap := NewEmptyVarMap()

		varMap.Set("Hello", "World")
		varMap.Set("Yo", true)

		Expect(varMap.Count()).To(Equal(2))
		varMap.Clear()
		Expect(varMap.Count()).To(Equal(0))

		result1, err := varMap.GetString("Hello")
		Expect(err).NotTo(BeNil())
		Expect(result1).To(Equal(""))

		result2, err := varMap.GetBool("Yo")
		Expect(err).NotTo(BeNil())
		Expect(result2).To(Equal(false))
	})

	It("Appends a secondary VarMap", func() {
		varMap1 := NewEmptyVarMap()

		varMap1.Set("Hello", "World")
		varMap1.Set("Yo", true)

		varMap2 := NewEmptyVarMap()

		varMap2.Set("Hello", "Universe")
		varMap2.Set("Da", 6543.21)

		varMap1.Append(varMap2)

		result1, err := varMap1.GetString("Hello")
		Expect(err).To(BeNil())
		Expect(result1).To(Equal("Universe"))

		result2, err := varMap1.GetBool("Yo")
		Expect(err).To(BeNil())
		Expect(result2).To(Equal(true))

		result3, err := varMap1.GetFloat64("Da")
		Expect(err).To(BeNil())
		Expect(result3).To(Equal(6543.21))
	})

	It("Gets the first match from multiple VarMaps", func() {
		varMap := NewEmptyVarMap()

		varMap.Set("Hello", "World")
		varMap.Set("Yo", true)
		varMap.Set("Mo", "Lo")
		varMap.Set("Da", 1234.12)
		varMap.Set("Ba", 6543.43)

		result1, err := varMap.GetFirstMatchString("Zello", "Tello", "Hello", "Mo")
		Expect(err).To(BeNil())
		Expect(result1).To(Equal("World"))

		result2, err := varMap.GetFirstMatchString("Zello", "Da", "Ba", "Mo", "Hello", "Zolo")
		Expect(err).To(BeNil())
		Expect(result2).To(Equal("Lo"))

		result3, err := varMap.GetFirstMatchFloat64("Hello", "Da", "Ba", "Hello", "Yo", "Zolo")
		Expect(err).To(BeNil())
		Expect(result3).To(Equal(1234.12))

		result4, err := varMap.GetFirstMatchInt64("Hello", "Ba", "Da", "Tello", "Yo", "Zolo")
		Expect(err).To(BeNil())
		Expect(result4).To(Equal(int64(6543)))
		
		result5, err := varMap.GetFirstMatchBool("Hello", "Ba", "Da", "Tello", "Yo", "Zolo")
		Expect(err).To(BeNil())
		Expect(result5).To(Equal(true))
	})

	It("Can be initialized from a regular Map", func() {
		someMap := map[string]interface{} {
			"a": 1234.12,
			"b": "Hello",
			"c": true,
			"d": nil,
		}

		varMap := NewVarMap(someMap)

		result1, err := varMap.GetFloat64("a")
		Expect(err).To(BeNil())
		Expect(result1).To(Equal(1234.12))

		result2, err := varMap.GetInt64("a")
		Expect(err).To(BeNil())
		Expect(result2).To(Equal(int64(1234)))

		result3, err := varMap.GetString("b")
		Expect(err).To(BeNil())
		Expect(result3).To(Equal("Hello"))

		result4, err := varMap.GetBool("c")
		Expect(err).To(BeNil())
		Expect(result4).To(Equal(true))

		result5, err := varMap.GetAny("d")
		Expect(err).To(BeNil())
		Expect(result5).To(BeNil())		
	})
})

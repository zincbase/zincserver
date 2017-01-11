package main

import (
	"bytes"
	"errors"
	"strconv"
	//"log"
	"encoding/base64"
	"fmt"
	"io"
)

type EntryStreamFormatter struct {
	entryStreamIterator EntryStreamIteratorFunc
	format              string
	buffer              *bytes.Buffer
	firstEntryWritten   bool
	eofReached          bool
}

func (this *EntryStreamFormatter) Read(p []byte) (n int, err error) {
	err = this.fillIfNeeded(len(p))

	if err != nil {
		return 0, err
	}

	return this.buffer.Read(p)
}

func (this *EntryStreamFormatter) fillIfNeeded(neededByteCount int) error {
	if this.eofReached {
		return nil
	}

	for this.buffer.Len() < neededByteCount {
		iteratorResult, err := this.entryStreamIterator()
		if err != nil {
			return err
		}

		if iteratorResult == nil {
			switch this.format {
			case "jsonArray":
				if !this.firstEntryWritten {
					this.buffer.WriteString("[\n]")
				} else {
					this.buffer.WriteString("\n]")
				}
			case "jsonObject":
				if !this.firstEntryWritten {
					this.buffer.WriteString("{\n}")
				} else {
					this.buffer.WriteString("\n}")
				}
			}

			this.eofReached = true
			return nil
		}

		writeKeyToBuffer := func() (err error) {
			key, err := iteratorResult.ReadKey()

			switch iteratorResult.PrimaryHeader.KeyFormat {
			case DataFormat_JSON:
				_, err = this.buffer.Write(key)
			case DataFormat_Binary:
				this.buffer.WriteByte('"')
				base64Encoder := base64.NewEncoder(base64.StdEncoding, this.buffer)
				_, err = base64Encoder.Write(key)
				err = base64Encoder.Close()
				this.buffer.WriteByte('"')
			case DataFormat_UTF8:
				this.buffer.WriteByte('"')
				jsonStringEscapeEncoder := NewJsonStringEscapeEncoder(this.buffer)
				_, err = jsonStringEscapeEncoder.Write(key)
				this.buffer.WriteByte('"')
			default:
				return errors.New(fmt.Sprintf("\n\n!FORMATTER ERROR: encountered an entry with a key having an unsupported encoding type #%d", iteratorResult.PrimaryHeader.KeyFormat))
			}

			return
		}

		writeValueToBuffer := func() (err error) {
			switch iteratorResult.PrimaryHeader.ValueFormat {
			case DataFormat_JSON:
				_, err = this.buffer.ReadFrom(iteratorResult.CreateValueReader())
			case DataFormat_Binary:
				this.buffer.WriteByte('"')
				base64Encoder := base64.NewEncoder(base64.StdEncoding, this.buffer)
				_, err = io.Copy(base64Encoder, iteratorResult.CreateValueReader())
				err = base64Encoder.Close()
				this.buffer.WriteByte('"')
			case DataFormat_UTF8:
				this.buffer.WriteByte('"')
				jsonStringEscapeEncoder := NewJsonStringEscapeEncoder(this.buffer)
				_, err = io.Copy(jsonStringEscapeEncoder, iteratorResult.CreateValueReader())
				this.buffer.WriteByte('"')
			default:
				return errors.New(fmt.Sprintf("\n\n!FORMATTER ERROR: encountered an entry with a value having an unsupported encoding type #%d", iteratorResult.PrimaryHeader.ValueFormat))
			}

			return
		}

		switch this.format {
		case "jsonArray":
			if !this.firstEntryWritten {
				this.buffer.WriteString("[\n\t{\n\t\t\"timestamp\": ")
			} else {
				this.buffer.WriteString(",\n\t{\n\t\t\"timestamp\": ")
			}

			this.buffer.WriteString(strconv.FormatInt(iteratorResult.PrimaryHeader.CommitTime, 10))
			this.buffer.WriteString(",\n\t\t\"key\": ")

			err = writeKeyToBuffer()
			if err != nil {
				return err
			}

			if iteratorResult.ValueSize() > 0 {
				this.buffer.WriteString(",\n\t\t\"value\": ")

				err = writeValueToBuffer()
				if err != nil {
					return err
				}
			}

			this.buffer.WriteString("\n\t}")
			this.firstEntryWritten = true

		case "jsonObject":
			if iteratorResult.ValueSize() > 0 {
				if !this.firstEntryWritten {
					this.buffer.WriteString("{\n\t")
				} else {
					this.buffer.WriteString(",\n\t")
				}

				err = writeKeyToBuffer()
				if err != nil {
					return err
				}

				this.buffer.WriteString(": ")

				err = writeValueToBuffer()
				if err != nil {
					return err
				}

				this.firstEntryWritten = true
			}

		case "jsonStream":
			this.buffer.WriteString("{\"timestamp\": ")
			this.buffer.WriteString(strconv.FormatInt(iteratorResult.PrimaryHeader.CommitTime, 10))
			this.buffer.WriteString(", \"key\": ")

			err = writeKeyToBuffer()
			if err != nil {
				return err
			}
			if iteratorResult.ValueSize() > 0 {
				this.buffer.WriteString(", \"value\": ")

				err = writeValueToBuffer()
				if err != nil {
					return err
				}
			}
			this.buffer.WriteString("}\n")

		case "tabbedJson":
			this.buffer.WriteString(strconv.FormatInt(iteratorResult.PrimaryHeader.CommitTime, 10))
			this.buffer.WriteByte('\t')

			err = writeKeyToBuffer()
			if err != nil {
				return err
			}

			this.buffer.WriteByte('\t')

			if iteratorResult.ValueSize() > 0 {
				err = writeValueToBuffer()
				if err != nil {
					return err
				}
			}

			this.buffer.WriteByte('\n')

		case "tabbedJsonShort":
			if iteratorResult.ValueSize() > 0 {
				err = writeKeyToBuffer()
				if err != nil {
					return err
				}
				this.buffer.WriteByte('\t')

				err = writeValueToBuffer()
				if err != nil {
					return err
				}

				this.buffer.WriteByte('\n')
			}
		}
	}

	return nil
}

func (this *EntryStreamFormatter) formatAndWriteEntry(header EntryPrimaryHeader, keyReader io.Reader, valueReader io.Reader) {

}

func NewEntryStreamFormatter(entryStreamIterator EntryStreamIteratorFunc, format string) *EntryStreamFormatter {
	return &EntryStreamFormatter{
		entryStreamIterator: entryStreamIterator,
		format:              format,
		buffer:              &bytes.Buffer{},
	}
}

func ParseFormattedEntries(formattedEntries []byte, format string) (parsedEntries []Entry, err error) {
	parseTabbedJson := func() (parsedEntries []Entry, err error) {
		currentLineNumber := func() string { return fmt.Sprintf("%d", len(parsedEntries)+1) }

		for len(formattedEntries) > 0 {
			nextSeperatorIndex := bytes.IndexByte(formattedEntries, '\t')

			if nextSeperatorIndex == -1 {
				return nil, errors.New("Error parsing formatted entries: tab character expected at line " + currentLineNumber())
			}
			timestamp, err := strconv.ParseInt(string(formattedEntries[0:nextSeperatorIndex]), 10, 64)
			if err != nil {
				return nil, err
			}

			formattedEntries = formattedEntries[nextSeperatorIndex+1:]
			nextSeperatorIndex = bytes.IndexByte(formattedEntries, '\t')

			if nextSeperatorIndex == -1 {
				return nil, errors.New("Error parsing formatted entries: tab character expected at line " + currentLineNumber())
			}

			key := formattedEntries[0:nextSeperatorIndex]

			formattedEntries = formattedEntries[nextSeperatorIndex+1:]
			nextSeperatorIndex = bytes.IndexByte(formattedEntries, '\n')

			if nextSeperatorIndex == -1 {
				return nil, errors.New("Error parsing formatted entries: newline character expected at line " + currentLineNumber())
			}

			value := formattedEntries[0:nextSeperatorIndex]

			formattedEntries = formattedEntries[nextSeperatorIndex+1:]

			entryHeader := &EntryPrimaryHeader{
				TotalSize:   int64(PrimaryHeaderSize + len(key) + len(value)),
				UpdateTime:  timestamp,
				CommitTime:  timestamp,
				KeySize:     uint16(len(key)),
				KeyFormat:   DataFormat_JSON,
				ValueFormat: DataFormat_JSON,
			}

			parsedEntries = append(parsedEntries, Entry{entryHeader, key, value})
		}

		return
	}

	parseTabbedJsonShort := func() (parsedEntries []Entry, err error) {
		timestamp := MonoUnixTimeMicro();

		currentLineNumber := func() string { return fmt.Sprintf("%d", len(parsedEntries)+1) }

		for len(formattedEntries) > 0 {
			nextSeperatorIndex := bytes.IndexByte(formattedEntries, '\t')

			if nextSeperatorIndex == -1 {
				return nil, errors.New("Error parsing formatted entries: tab character expected at line " + currentLineNumber())
			}

			key := formattedEntries[0:nextSeperatorIndex]

			formattedEntries = formattedEntries[nextSeperatorIndex+1:]
			nextSeperatorIndex = bytes.IndexByte(formattedEntries, '\n')

			if nextSeperatorIndex == -1 {
				return nil, errors.New("Error parsing formatted entries: newline character expected at line " + currentLineNumber())
			}

			value := formattedEntries[0:nextSeperatorIndex]

			formattedEntries = formattedEntries[nextSeperatorIndex+1:]

			entryHeader := &EntryPrimaryHeader{
				TotalSize:   int64(PrimaryHeaderSize + len(key) + len(value)),
				UpdateTime: timestamp,
				CommitTime: timestamp,
				KeySize:     uint16(len(key)),
				KeyFormat:   DataFormat_JSON,
				ValueFormat: DataFormat_JSON,
			}

			parsedEntries = append(parsedEntries, Entry{entryHeader, key, value})
		}

		return
	}

	switch format {
	case "tabbedJson":
		return parseTabbedJson()
	case "tabbedJsonShort":
		return parseTabbedJsonShort()
	default:
		return nil, errors.New("Invalid format")
	}
}

func SerializeFormattedEntries(formattedEntries []byte, format string) (serializedEntries []byte, err error) {
	if format == "" || format == "raw" {
		return formattedEntries, nil
	}

	parsedEntries, err := ParseFormattedEntries(formattedEntries, format)
	if err != nil {
		return nil, err
	}

	return SerializeEntries(parsedEntries), nil
}

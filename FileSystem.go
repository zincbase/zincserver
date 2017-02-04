package main

import (
	"errors"
	"io"
	"os"
)

func FileExists(filePath string) (bool, error) {
	_, err := os.Stat(filePath)

	if err == nil {
		return true, nil
	} else {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
}

func DirectoryExists(directoryPath string) (bool, error) {
	stats, err := os.Stat(directoryPath)

	if err == nil {
		if stats.IsDir() {
			return true, nil
		} else {
			return false, errors.New("The path '" + directoryPath + "' references a an existing file, not a directory.")
		}
	} else {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
}

func CreateOrRewriteFile(filePath string, newContentReader io.Reader, flushAfterWrite bool) (err error) {
	// Open the file with creation and truncate modes and delete sharing
	file, err := OpenFileWithDeleteSharing(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)

	// If an error occurred while opening the file
	if err != nil {
		// Return the error
		return
	}

	// Close the file once the function exists
	defer file.Close()

	// Write the data from the reader to the file
	_, err = io.Copy(file, newContentReader)

	// If an error occurred while writing
	if err != nil {
		// Return the error
		return
	}

	// If a flush was requested
	if flushAfterWrite {
		// Flush the file
		err = file.Sync()

		// If an error occurred while flushing the file
		if err != nil {
			// Return the error
			return
		}
	}

	return
}

func ReadEntireFile(filePath string) (fileContent []byte, err error) {
	// Open the file for reading only, with delete sharing enabled
	file, err := OpenFileWithDeleteSharing(filePath, os.O_RDONLY, 0666)

	// If an error occurred while opening the file
	if err != nil {
		// Return the error
		return
	}

	// Close the file once the function exists
	defer file.Close()

	// Create a new memory writer
	memoryWriter := NewMemoryWriter()

	// Read the entire file to memory
	_, err = io.Copy(memoryWriter, file)

	// If an error occurred while reading the file
	if err != nil {
		// Return the error
		return
	}

	// Set the returned slice to the data read
	fileContent = memoryWriter.WrittenData()

	return
}

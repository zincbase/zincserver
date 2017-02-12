package main

import (
	"errors"
	"io"
	"os"
	"fmt"
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

	// Read the entire file to memory
	fileContent, err = ReadEntireStream(file)

	return
}

// Safely creates or rewrites a file
func CreateOrRewriteFileSafe(filePath string, newContentReader io.Reader) (err error) {
	// Initialize temporary file names
	tempFileName := fmt.Sprintf("%s.partial-%d", filePath, MonoUnixTimeMicro())

	// Write the new content to a temporary file, and flush it afterwards
	err = CreateOrRewriteFile(tempFileName, newContentReader, true)

	// If an error occurred while writing to the temporary file
	if err != nil {
		// Return the error
		return
	}

	// Replace the target file with the temporary file
	return ReplaceFileSafe(tempFileName, filePath)
}

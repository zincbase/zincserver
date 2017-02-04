// +build windows

package main

import (
	"fmt"
	"io"
	"os"
)

func CreateOrRewriteFileSafe(filePath string, newContentReader io.Reader) (err error) {
	// Get timestamp
	timestamp := MonoUnixTimeMicro()

	// Initialize temporary file names
	tempFileName := fmt.Sprintf("%s.partial-%d", filePath, timestamp)
	oldFileName := fmt.Sprintf("%s.old-%d", filePath, timestamp)

	// Write the new content to a temporary file
	err = CreateOrRewriteFile(tempFileName, newContentReader, true)

	// If an error occurred while creating or rewriting the file
	if err != nil {
		// Return the error
		return
	}

	// Check if a file with the given path already exists.
	fileExists, err := FileExists(filePath)

	// If an error occurred while checking the existence of the file
	if err != nil {
		// Return the error
		return
	}

	// If the file exists
	if fileExists {
		// Rename the existing file to the 'old' file
		err = os.Rename(filePath, oldFileName)

		// If an error occurred while renaming the existing file
		if err != nil {
			// Return the error
			return
		}
	}

	// Rename the temporary file into the target file
	err = os.Rename(tempFileName, filePath)

	// If an error occurred while renaming the temporary file
	if err != nil {
		// Return the error
		return
	}

	// Unlink the old file, if existed
	if fileExists {
		// Unlink the old file
		err = os.Remove(oldFileName)

		// If an error occurred while unlinking the old file
		if err != nil {
			// Return the error
			return
		}
	}

	return
}

func UnlinkFileSafe(filePath string) (err error) {
	deletedFileName := fmt.Sprintf("%s.deleted-%d", filePath, MonoUnixTimeMicro())
	err = os.Rename(filePath, deletedFileName)
	if err != nil {
		return
	}

	err = os.Remove(deletedFileName)
	if err != nil {
		return
	}

	return
}

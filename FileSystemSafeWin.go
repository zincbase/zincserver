// +build windows

package main

import (
	"fmt"
	"os"
)

// Safely replaces a file with another file, if exists. On windows this can't be done
// with a single Rename operation, even if the existing file is opened with delete sharing mode.
func ReplaceFileSafe(sourceFilePath string, targetFilePath string) (err error) {
	tempFileName := fmt.Sprintf("%s.old-%d", targetFilePath, MonoUnixTimeMicro())

	// Check if the target file already exists
	targetFileExisted, err := FileExists(targetFilePath)

	// If an error occurred while checking for the existence of the file
	if err != nil {
		return
	}

	// If the target already exists
	if targetFileExisted {
		// Rename the existing file to a temporary name.
		err = os.Rename(targetFilePath, tempFileName)

		// If an error occurred
		if err != nil {
			// Return the error
			return
		}
	}

	// Rename the source file to the target file
	err = os.Rename(sourceFilePath, targetFilePath)

	// If an error occurred while renaming the file
	if err != nil {
		// Return the error
		return
	}

	// If the target file existed
	if targetFileExisted {
		// Unlink the temporary file
		err = os.Remove(tempFileName)

		// If an error occurred while deleting the file
		if err != nil {
			// Return the error
			return
		}
	}

	return
}

func UnlinkFileSafe(filePath string) (err error) {
	// Initialize deleted file temporary file name
	deletedFileName := fmt.Sprintf("%s.deleted-%d", filePath, MonoUnixTimeMicro())

	// Rename the file to the temporary file name
	err = os.Rename(filePath, deletedFileName)
	if err != nil {
		return
	}

	// Delete the file
	err = os.Remove(deletedFileName)
	if err != nil {
		return
	}

	return
}

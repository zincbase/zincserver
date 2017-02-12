// +build !windows

package main

import "os"

func ReplaceFileSafe(sourceFilePath string, targetFilePath string) error {
	// Rename the file, any errors that occurred during the operations would be returned
	return os.Rename(sourceFilePath, targetFilePath)
}

func UnlinkFileSafe(filePath string) error {
	// Unlink the file, any errors that occurred during the deletion would be returned
	return os.Remove(filePath)
}

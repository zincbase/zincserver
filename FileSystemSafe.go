// +build !windows

package main

import (
	"fmt"
	"io"
	"os"
)

func CreateOrRewriteFileSafe(filePath string, newContentReader io.Reader) (err error) {
	// Initialize a temporary file name
	tempFileName := fmt.Sprintf("%s.partial-%d", filePath, MonoUnixTimeMicro())

	// Write new content to a temporary file
	err = CreateOrRewriteFile(tempFileName, newContentReader, true)

	// If an error occurred while creating or rewriting the file
	if err != nil {
		// Return the error
		return
	}

	// Rename the temporary file to the target file
	err = os.Rename(tempFileName, filePath)

	/*
	if err != nil {
		if errObject, ok := err.(*os.LinkError); ok {
			if errno, ok := errObject.Err.(syscall.Errno); ok {
				fmt.Println(uintptr(errno))
			}
		}
	}
	*/

	// Any errors that occurred during the rename would be returned
	return
}

func UnlinkFileSafe(filePath string) (err error) {
	// Unlink the file
	err = os.Remove(filePath)

	// Any errors that occurred during the deletion would be returned
	return
}

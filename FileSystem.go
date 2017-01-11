package main

import (
	"fmt"
	"io"
	"os"
	"errors"
)

func ReplaceFileSafely(filePath string, newContentReader io.Reader) (err error) {
	// Write new content to a temporary file
	tempFileName := fmt.Sprintf("%s.temp-%d", filePath, MonoUnixTimeMicro())
	RewriteFile(tempFileName, newContentReader, true)

	// Add the '.old' suffix to the existing file, if exists.
	oldFileName := fmt.Sprintf("%s.old-%d", filePath, MonoUnixTimeMicro())
	fileExists, err := FileExists(filePath)
	if err != nil {
		return
	}

	if fileExists {
		err = RenameFile(filePath, oldFileName)
		if err != nil {
			return
		}
	}

	// Rename the temporary file to the target file
	err = RenameFile(tempFileName, filePath)
	if err != nil {
		return
	}

	// Delete the old file, if existed
	if fileExists {
		err = os.Remove(oldFileName)
		if err != nil {
			return
		}
	}

	return
}

func DeleteFileSafely(filePath string) (err error) {
	deletedFileName := fmt.Sprintf("%s.deleted-%d", filePath, MonoUnixTimeMicro())
	err = RenameFile(filePath, deletedFileName)
	if err != nil {
		return
	}

	err = os.Remove(deletedFileName)
	if err != nil {
		return
	}

	return
}

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

func RewriteFile(filePath string, newContentReader io.Reader, syncAfterWrite bool) (err error) {
	file, err := OpenFileWithDeleteSharing(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return
	}

	defer file.Close()

	_, err = io.Copy(file, newContentReader)
	if err != nil {
		return
	}

	if syncAfterWrite {
		err = file.Sync()
		if err != nil {
			return
		}
	}

	return
}

func ReadEntireFile(filePath string) (fileContent []byte, err error) {
	file, err := OpenFileWithDeleteSharing(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return
	}

	defer file.Close()

	memoryWriter := NewMemoryWriter()
	_, err = io.Copy(memoryWriter, file)
	if err != nil {
		return
	}

	fileContent = memoryWriter.WrittenData()

	return
}

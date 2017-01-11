// +build !windows

package main

import (
	"os"
)

func OpenFileWithDeleteSharing(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}
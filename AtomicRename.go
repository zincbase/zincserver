// +build !windows

package main

import "os"

func RenameFile(src, dst string) error {
	return os.Rename(src, dst)
}
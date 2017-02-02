// +build windows

package main

import (
	"os"
	"syscall"
	"unsafe"
)

func openWithDeleteSharing(path string, mode int, perm uint32) (fd syscall.Handle, err error) {
	if len(path) == 0 {
		return syscall.InvalidHandle, syscall.ERROR_FILE_NOT_FOUND
	}
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return syscall.InvalidHandle, err
	}
	var access uint32
	switch mode & (syscall.O_RDONLY | syscall.O_WRONLY | syscall.O_RDWR) {
	case syscall.O_RDONLY:
		access = syscall.GENERIC_READ
	case syscall.O_WRONLY:
		access = syscall.GENERIC_WRITE
	case syscall.O_RDWR:
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	}
	if mode&syscall.O_CREAT != 0 {
		access |= syscall.GENERIC_WRITE
	}
	if mode&syscall.O_APPEND != 0 {
		access &^= syscall.GENERIC_WRITE
		access |= syscall.FILE_APPEND_DATA
	}

	//sharemode := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE)
	sharemode := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_DELETE)

	var sa *syscall.SecurityAttributes
	if mode&syscall.O_CLOEXEC == 0 {
		sa = &syscall.SecurityAttributes{
			Length:        uint32(unsafe.Sizeof(sa)),
			InheritHandle: 1,
		}

		//sa = makeInheritSa()
	}
	var createmode uint32
	switch {
	case mode&(syscall.O_CREAT|syscall.O_EXCL) == (syscall.O_CREAT | syscall.O_EXCL):
		createmode = syscall.CREATE_NEW
	case mode&(syscall.O_CREAT|syscall.O_TRUNC) == (syscall.O_CREAT | syscall.O_TRUNC):
		createmode = syscall.CREATE_ALWAYS
	case mode&syscall.O_CREAT == syscall.O_CREAT:
		createmode = syscall.OPEN_ALWAYS
	case mode&syscall.O_TRUNC == syscall.O_TRUNC:
		createmode = syscall.TRUNCATE_EXISTING
	default:
		createmode = syscall.OPEN_EXISTING
	}
	h, e := syscall.CreateFile(pathp, access, sharemode, sa, createmode, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	return h, e
}

func OpenFileWithDeleteSharing(name string, flag int, perm os.FileMode) (*os.File, error) {
	syscallMode := func(i os.FileMode) (o uint32) {
		o |= uint32(i.Perm())
		if i&os.ModeSetuid != 0 {
			o |= syscall.S_ISUID
		}
		if i&os.ModeSetgid != 0 {
			o |= syscall.S_ISGID
		}
		if i&os.ModeSticky != 0 {
			o |= syscall.S_ISVTX
		}
		// No mapping for Go's ModeTemporary (plan9 only).
		return
	}

	if name == "" {
		return nil, &os.PathError{Op: "open", Path: name, Err: syscall.ENOENT}
	}

	r, errf := openWithDeleteSharing(name, flag|syscall.O_CLOEXEC, syscallMode(perm))
	if errf != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: errf}
	}
	return os.NewFile(uintptr(r), name), nil
}

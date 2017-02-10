package main

import (
	"errors"
	_ "log"
	"os"
	"sync"
)

type FileDescriptorCounterMap struct {
	counterMap map[uintptr]int
	sync.Mutex
}

var FileDescriptors *FileDescriptorCounterMap

func init() {
	FileDescriptors = &FileDescriptorCounterMap{counterMap: make(map[uintptr]int)}
}

func (this *FileDescriptorCounterMap) OpenAndIncrement(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	this.Lock()
	file, err = OpenFileWithDeleteSharing(name, flag, perm)

	if err == nil {
		this.counterMap[file.Fd()] = 1
	}

	//Logf("OpenAndIncrement %d", file.Fd())
	this.Unlock()
	return
}

func (this *FileDescriptorCounterMap) Increment(file *os.File) error {
	this.Lock()
	defer this.Unlock()

	fd := file.Fd()
	//Logf("Increment %d", fd)

	if this.counterMap[fd] == 0 {
		return errors.New("Attempt to increment 0 counter without opening a file")
	}

	this.counterMap[fd]++

	return nil
}

func (this *FileDescriptorCounterMap) Decrement(file *os.File) (err error) {
	this.Lock()
	defer this.Unlock()

	fd := file.Fd()
	//Logf("Decrement %d", fd)

	if this.counterMap[fd] == 0 {
		return errors.New("Attempt to decrement a 0 counter")
	}

	this.counterMap[fd]--

	if this.counterMap[fd] == 0 {
		delete(this.counterMap, fd)
		err = file.Close()
		//Logf("Close %d", fd)
	}

	return
}

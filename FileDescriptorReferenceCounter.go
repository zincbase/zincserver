package main

import (
	"os"
	"sync"
	_ "log"
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
	
	//log.Printf("OpenAndIncrement %d", file.Fd())
	this.Unlock()
	return
}

func (this *FileDescriptorCounterMap) Increment(file *os.File) {
	this.Lock()
	fd := file.Fd()
	//log.Printf("Increment %d", fd)

	if this.counterMap[fd] == 0 {
		panic("Attempt to increment 0 counter without opening a file")
	}
	this.counterMap[fd]++
	this.Unlock()
}

func (this *FileDescriptorCounterMap) Decrement(file *os.File) (err error) {
	this.Lock()
	fd := file.Fd()
	//log.Printf("Decrement %d", fd)

	if this.counterMap[fd] == 0 {
		panic("Attempt to decrement a 0 counter")
	}

	this.counterMap[fd]--

	if this.counterMap[fd] == 0 {
		delete(this.counterMap, fd)
		err = file.Close()
		//log.Printf("Close %d", fd)
	}
	this.Unlock()
	return
}

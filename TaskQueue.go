package main

import (
	"sync"
)

// The task queue object
type TaskQueue struct {
	taskChan chan func()
}

func NewTaskQueue() *TaskQueue {
	taskQueue := &TaskQueue{
		taskChan: make(chan func()),
	}

	go func() {
		for {
			taskFunc := <-taskQueue.taskChan
			taskFunc()
		}
	}()

	return taskQueue
}

func (this *TaskQueue) Exec(taskFunc func()) {
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)

	wrappedTaskFunc := func() {
		taskFunc()
		waitGroup.Done()
	}

	this.taskChan <- wrappedTaskFunc
	waitGroup.Wait()
}

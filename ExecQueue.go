package main

import (
	"sync"
)

type ExecQueueToken struct {
	endWaitGroup *sync.WaitGroup
	hasEnded     bool
}

type ExecQueue struct {
	waitChan chan *sync.WaitGroup
}

func NewExecQueue() *ExecQueue {
	queue := &ExecQueue{
		waitChan: make(chan *sync.WaitGroup),
	}

	go func() {
		for {
			waitGroup := <-queue.waitChan
			waitGroup.Wait()
		}
	}()

	return queue
}

func (this *ExecQueue) Enter() *ExecQueueToken {
	token := &ExecQueueToken{
		endWaitGroup: &sync.WaitGroup{},
		hasEnded:     false,
	}

	token.endWaitGroup.Add(1)
	this.waitChan <- token.endWaitGroup
	return token
}

func (this *ExecQueue) Leave(token *ExecQueueToken) {
	if !token.hasEnded {
		token.hasEnded = true
		token.endWaitGroup.Done()
	}
}

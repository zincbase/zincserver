package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sync"
	"time"
)

var _ = Describe("ExecQueue", func() {
	It("Runs a single task", func() {
		execQueue := NewExecQueue()
		x := 1
		token := execQueue.Enter()
		x = 2
		execQueue.Leave(token)
		Expect(x).To(Equal(2))
	})

	It("Serializes multiple tasks", func() {
		execQueue := NewExecQueue()

		x := 1
		waitGroup := &sync.WaitGroup{}
		waitGroup.Add(2)

		go func() {
			token := execQueue.Enter()
			time.Sleep(20)
			x = 2
			execQueue.Leave(token)

			waitGroup.Done()
		}()

		go func() {
			time.Sleep(5)
			token := execQueue.Enter()
			x = 3
			execQueue.Leave(token)

			waitGroup.Done()
		}()

		waitGroup.Wait()
		Expect(x).To(Equal(3))
	})
})

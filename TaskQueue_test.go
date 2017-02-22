package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("TaskQueue", func() {
	It("Runs a single task", func() {
		taskQueue := NewTaskQueue()

		a := 1
		taskQueue.Exec(func() {
			a = 2
		})
		Expect(a).To(Equal(2))
	})

	It("Serializes several tasks", func() {
		taskQueue := NewTaskQueue()

		numbers := []int{}

		for i := 0; i < 10; i++ {
			iteration := i

			taskQueue.Exec(func() {
				time.Sleep(time.Duration(10-iteration) * time.Millisecond)
				numbers = append(numbers, iteration)
			})
		}

		Expect(numbers).To(Equal([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}))
	})
})

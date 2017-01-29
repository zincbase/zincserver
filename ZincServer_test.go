package main

import (
	"testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func Test_ZincServer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ZincServer")
}

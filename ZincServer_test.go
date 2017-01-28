package main

import (
    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"	
	"testing"
)

func Test_ZincServer(t *testing.T) {
    RegisterFailHandler(Fail)
    RunSpecs(t, "ZincServer")
}
// +build !windows

package main

import (
	"time"
	_ "unsafe"
)

// monotime returns the current time in nanoseconds from a monotonic clock.
// The time returned is based on some arbitrary platform-specific point in the
// past. The time returned is guaranteed to increase monotonically at a
// constant rate, unlike time.Now() from the Go standard library, which may
// slow down, speed up, jump forward or backward, due to NTP activity or leap
// seconds.

//go:noescape
//go:linkname monotime runtime.nanotime
func monotime() int64

var MonoUnixTimeNano func() int64

func init() {
	// Record a base compensation value
	monoUnixTimestampBase := time.Now().UnixNano() - monotime()

	MonoUnixTimeNano = func() int64 {
		return monoUnixTimestampBase + monotime()
	}	
}

// Monotonic unix time, in integer nanoseconds
func MonoUnixTimeMicro() int64 {
	return MonoUnixTimeNano() / 1000
}

// Monotonic unix time, in integer milliseconds
func MonoUnixTimeMilli() int64 {
	return MonoUnixTimeNano() / 1000000
}

// Monotonic unix time, in floating point milliseconds
func MonoUnixTimeMilliFloat() float64 {
	return float64(MonoUnixTimeNano()) / 1000000
}
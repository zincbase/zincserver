// +build windows

package main

import (
	"syscall"
	"time"
	"unsafe"
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

func init() {
	TryInitializeQPCTimer := func() func() int64 {
		lib, err := syscall.LoadLibrary("kernel32.dll")
		if err != nil {
			return nil
		}

		qpc, _ := syscall.GetProcAddress(lib, "QueryPerformanceCounter")
		qpf, _ := syscall.GetProcAddress(lib, "QueryPerformanceFrequency")
		if qpc == 0 || qpf == 0 {
			return nil
		}

		var freq uint64 // Equals to 2929687 on my dev PC
		syscall.Syscall(qpf, 1, uintptr(unsafe.Pointer(&freq)), 0, 0)
		if freq <= 0 {
			return nil
		}

		var start uint64
		syscall.Syscall(qpc, 1, uintptr(unsafe.Pointer(&start)), 0, 0)

		return func() int64 {
			var now uint64
			syscall.Syscall(qpc, 1, uintptr(unsafe.Pointer(&now)), 0, 0)
			return int64(float64(now-start) / float64(freq) * 1000000000)
		}
	}

	// Will fail on non-windows systems or when performance counters are not available:
	timeFunc := TryInitializeQPCTimer()

	// If not running in windows or initialization failed, for some reason,
	// fall back to Go's internal monotonic time implementation:
	if timeFunc == nil {
		timeFunc = monotime
	}

	// Record a base compensation value
	monoUnixTimestampBase := time.Now().UnixNano() - timeFunc()

	// Set the resulting functions
	MonoUnixTimeNano = func() int64 {
		return monoUnixTimestampBase + timeFunc()
	}
}

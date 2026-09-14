//go:build !js && (darwin || linux)

package connect

import "syscall"

// testingOpenFileDescriptorFallback handles hosts where /dev/fd or
// /proc/self/fd is not enumerable. F_GETFD still works at the descriptor
// ceiling, which is the failure this diagnostic distinguishes.
func testingOpenFileDescriptorFallback() int {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return -1
	}
	count := 0
	for fd := uintptr(0); fd < uintptr(limit.Cur); fd++ {
		if _, _, errno := syscall.Syscall(
			syscall.SYS_FCNTL,
			fd,
			uintptr(syscall.F_GETFD),
			0,
		); errno == 0 {
			count++
		}
	}
	return count
}

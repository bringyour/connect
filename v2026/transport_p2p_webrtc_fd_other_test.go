//go:build !js && !darwin && !linux

package connect

// testingOpenFileDescriptorFallback reports unsupported on targets without the
// Unix rlimit/fcntl diagnostic. The WebRTC tests still compile and retain every
// non-descriptor assertion on those targets.
func testingOpenFileDescriptorFallback() int {
	return -1
}

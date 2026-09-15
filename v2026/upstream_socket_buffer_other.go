//go:build !linux && !darwin && !ios

package connect

// No known maxima: never pin, leave both buffers to the kernel.
func readSocketBufferPolicy() socketBufferPolicy {
	return socketBufferPolicy{}
}

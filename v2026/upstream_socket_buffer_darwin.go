//go:build darwin || ios

package connect

import "syscall"

// Darwin clamps an explicit request to kern.ipc.maxsockbuf and stores it as
// is; autoscaling reaches net.inet.tcp.auto{snd,rcv}bufmax.
func readSocketBufferPolicy() socketBufferPolicy {
	maxSockBuf, err1 := syscall.SysctlUint32("kern.ipc.maxsockbuf")
	sendCeiling, err2 := syscall.SysctlUint32("net.inet.tcp.autosndbufmax")
	receiveCeiling, err3 := syscall.SysctlUint32("net.inet.tcp.autorcvbufmax")
	if err1 != nil || err2 != nil || err3 != nil {
		return socketBufferPolicy{}
	}
	return socketBufferPolicy{
		known:                   true,
		sendCoreMaxByteCount:    int(maxSockBuf),
		receiveCoreMaxByteCount: int(maxSockBuf),
		sendCeilingByteCount:    int(sendCeiling),
		receiveCeilingByteCount: int(receiveCeiling),
	}
}

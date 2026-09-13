//go:build linux

package connect

import (
	"os"
	"strconv"
	"strings"
)

// Linux clamps an explicit request to net.core.{w,r}mem_max and doubles
// it; autotuning reaches tcp_{w,r}mem's maximum, the third value.
func readSocketBufferPolicy() socketBufferPolicy {
	readInts := func(path string) ([]int, bool) {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, false
		}
		var values []int
		for _, field := range strings.Fields(string(content)) {
			value, err := strconv.Atoi(field)
			if err != nil {
				return nil, false
			}
			values = append(values, value)
		}
		return values, 0 < len(values)
	}
	sendCoreMax, ok1 := readInts("/proc/sys/net/core/wmem_max")
	receiveCoreMax, ok2 := readInts("/proc/sys/net/core/rmem_max")
	sendRange, ok3 := readInts("/proc/sys/net/ipv4/tcp_wmem")
	receiveRange, ok4 := readInts("/proc/sys/net/ipv4/tcp_rmem")
	if !ok1 || !ok2 || !ok3 || !ok4 || len(sendRange) < 3 || len(receiveRange) < 3 {
		return socketBufferPolicy{}
	}
	return socketBufferPolicy{
		known:                   true,
		doubled:                 true,
		sendCoreMaxByteCount:    sendCoreMax[0],
		receiveCoreMaxByteCount: receiveCoreMax[0],
		sendCeilingByteCount:    sendRange[2],
		receiveCeilingByteCount: receiveRange[2],
	}
}

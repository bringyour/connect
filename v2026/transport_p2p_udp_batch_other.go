//go:build !linux && !js

// Platforms without sendmmsg keep the same bounded pipeline and issue the
// gathered writes in order. The independent writer still overlaps socket I/O
// with Transfer and encryption work.
package connect

import "github.com/pion/transport/v4"

// newP2pUdpBatchWriter creates the sequential platform implementation.
func newP2pUdpBatchWriter(
	connection transport.UDPConn,
	network string,
	batchSize int,
) p2pUdpBatchWriter {
	_ = network
	_ = batchSize
	return &p2pUdpBatchWriterFallback{connection: connection}
}

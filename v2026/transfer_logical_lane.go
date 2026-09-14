package connect

// Transfer logical lanes isolate independent IP flows from a missing Pack in
// another flow without creating an unbounded sequence per five-tuple. Lane 0
// is always the legacy/control lane; data lanes are numbered 1..8.
const (
	transferLogicalLaneVersion uint32 = 1
	maxLogicalDataLaneCount           = 8
)

// logicalLaneForCount hashes one exact directional five-tuple into the bounded
// data-lane range. The fixed FNV-1a encoding is stable across processes and Go
// versions; map iteration or a process-random hash must not move a live flow.
func (self sendSchedulingKey) logicalLaneForCount(count int) uint32 {
	if !self.valid || count <= 0 {
		return 0
	}
	count = min(count, maxLogicalDataLaneCount)
	hash := uint64(14695981039346656037)
	add := func(value byte) {
		hash ^= uint64(value)
		hash *= 1099511628211
	}
	for _, value := range self.ipFlow.sourceIp {
		add(value)
	}
	for _, value := range self.ipFlow.destinationIp {
		add(value)
	}
	add(byte(self.ipFlow.protocol))
	add(byte(self.ipFlow.sourcePort >> 8))
	add(byte(self.ipFlow.sourcePort))
	add(byte(self.ipFlow.destinationPort >> 8))
	add(byte(self.ipFlow.destinationPort))
	add(self.ipFlow.ipVersion)
	return 1 + uint32(hash%uint64(count))
}

// Nonzero lanes share one fixed amount of channel headroom. Dividing by the
// maximum supported lane count means enabling eight lanes adds at most one
// legacy lane's capacity rather than multiplying it eightfold.
func logicalLaneSequenceBufferSize(size int, logicalLane uint32) int {
	if logicalLane == 0 || size <= 0 {
		return size
	}
	return max(1, (size+maxLogicalDataLaneCount-1)/maxLogicalDataLaneCount)
}

//go:build !android && !ios

package connect

// Desktop/server paths do not run mobile packet-pressure admission. Their
// Get/Return hot path therefore retains no extra live-root bookkeeping; an
// occasional diagnostic query falls back to the cumulative tag counters.
const messagePoolTrackPacketOutstanding = false

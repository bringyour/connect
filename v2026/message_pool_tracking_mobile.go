//go:build android || ios

package connect

// Only constrained mobile embedders run the packet-pressure sampler. Keeping
// this a build constant lets desktop/server compilers remove the per-root live
// counter writes entirely.
const messagePoolTrackPacketOutstanding = true

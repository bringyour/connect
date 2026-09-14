//go:build race

package connect

// raceEnabled reports whether the test binary was built with -race. Under -race the
// gvisor stack runs orders of magnitude slower, so throughput/timing assertions that
// are meaningful at full speed (e.g. TestTunTCPThroughput's byte count and floor) are
// scaled down to still exercise the path within the per-connection deadlines.
const raceEnabled = true

#!/usr/bin/env python3
"""Env-gated experiment knobs (NEVER committed):
   URNETWORK_NO_P2P=1        -> hard-disable direct/p2p mode (relay-only arm)
   URNETWORK_WIN_MIB=<n>     -> sender resend window = n MiB
   URNETWORK_RWIN_MIB=<n>    -> receiver queue      = n MiB
"""
import sys, pathlib, re
root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else '.')

# --- transfer.go: env-scaled windows
t = root / 'transfer.go'
s = t.read_text()
assert 'envExperimentByteCount' not in s, 'already patched'
old_send = "\t\tResendQueueMaxByteCount: MemoryScaledByteCount(mib(2), kib(256)),"
old_recv = "\t\tReceiveQueueMaxByteCount: MemoryScaledByteCount(mib(2)+kib(512), kib(320)),"
old_lane = "\t\tLogicalDataLaneCount: 0,"
assert old_send in s and old_recv in s, 'window anchors not found'
s = s.replace(old_send, "\t\tResendQueueMaxByteCount: envExperimentByteCount(\"URNETWORK_WIN_MIB\", MemoryScaledByteCount(mib(2), kib(256))),")
s = s.replace(old_recv, "\t\tReceiveQueueMaxByteCount: envExperimentByteCount(\"URNETWORK_RWIN_MIB\", MemoryScaledByteCount(mib(2)+kib(512), kib(320))),")
assert old_lane in s, 'lane anchor not found'
s = s.replace(old_lane, "\t\tLogicalDataLaneCount: envExperimentCount(\"URNETWORK_LANES\", 0),")
helper = '''
// envExperimentByteCount is a DIAGNOSTIC-ONLY override (never committed).
func envExperimentByteCount(name string, def ByteCount) ByteCount {
	if v := envpkg.Getenv(name); v != "" {
		if n, err := strconvpkg.Atoi(v); err == nil && 0 < n {
			return ByteCount(n) * 1024 * 1024
		}
	}
	return def
}

// envExperimentCount is a DIAGNOSTIC-ONLY override (never committed).
func envExperimentCount(name string, def int) int {
	if v := envpkg.Getenv(name); v != "" {
		if n, err := strconvpkg.Atoi(v); err == nil && 0 <= n {
			return n
		}
	}
	return def
}
'''
s = s.replace('import (\n\t"context"', 'import (\n\tenvpkg "os"\n\tstrconvpkg "strconv"\n\n\t"context"', 1)
s = s.rstrip() + '\n' + helper
t.write_text(s)

# --- ip_remote_multi_client.go: env-gated no-p2p
m = root / 'ip_remote_multi_client.go'
s = m.read_text()
assert 'URNETWORK_NO_P2P' not in s, 'already patched'
anchor = """	if self.settings.OverrideAllowDirect != nil {
		performanceProfile = forceAllowDirect(performanceProfile, *self.settings.OverrideAllowDirect)
	}
	return performanceProfile"""
assert anchor in s, 'allow-direct anchor not found'
s = s.replace(anchor, """	if self.settings.OverrideAllowDirect != nil {
		performanceProfile = forceAllowDirect(performanceProfile, *self.settings.OverrideAllowDirect)
	}
	if envpkg2.Getenv("URNETWORK_NO_P2P") == "1" {
		performanceProfile = forceAllowDirect(performanceProfile, false)
	}
	return performanceProfile""")
s = s.replace('import (\n\t"context"', 'import (\n\tenvpkg2 "os"\n\n\t"context"', 1)
m.write_text(s)
print("patched:", t.name, m.name)

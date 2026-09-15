#!/usr/bin/env python3
"""DIAGNOSTIC ONLY (never committed).
URNETWORK_TDIAG=1: TIME-weighted SendSequence gate accounting, per sequence, per 1 s:
  noCap   = wall time the loop waits with ingress closed because the resend window is full
  otherNil= wall time waiting with ingress closed for any other reason
  starve  = wall time waiting with ingress open (no packs to send)
  work    = everything else (ack apply, contract, route write)
URNETWORK_ACK_COMPRESS_US=<n>: receive-side AckCompressTimeout override (microseconds, 0 allowed via 'off')."""
import sys, pathlib
root = pathlib.Path(sys.argv[1])
t = root / 'transfer.go'; s = t.read_text()
assert 'TDIAG' not in s
sel = "\t\tidleTimer.Reset(timeout)\n\t\tselect {\n\t\tcase <-self.ctx.Done():\n\t\t\tif !flightWaitStart.IsZero() {"
assert s.count(sel) == 1, 'select anchor'
s = s.replace(sel, """\t\tvar tdSelStart time.Time
\t\ttdClass := 0
\t\tif tdiagEnabled {
\t\t\ttdSelStart = time.Now()
\t\t\tif packIngress == nil {
\t\t\t\tif !resendCapacity {
\t\t\t\t\ttdClass = 1
\t\t\t\t} else {
\t\t\t\t\ttdClass = 2
\t\t\t\t}
\t\t\t}
\t\t\t_, tdRqBytes := self.resendQueue.QueueSize()
\t\t\tself.tdRqSum += int64(tdRqBytes)
\t\t\tself.tdRqN += 1
\t\t\tif self.tdRqMax < int64(tdRqBytes) {
\t\t\t\tself.tdRqMax = int64(tdRqBytes)
\t\t\t}
\t\t}
""" + sel, 1)
end = "\t\tif !flightWaitStart.IsZero() {\n\t\t\tself.client.observeUnreliableFlightWait(time.Since(flightWaitStart))\n\t\t}\n\t\tif packsClosed {\n\t\t\treturn\n\t\t}\n\t}\n}\n"
assert s.count(end) == 1, 'end anchor'
s = s.replace(end, """\t\tif tdiagEnabled {
\t\t\ttdNow := time.Now()
\t\t\ttdWait := tdNow.Sub(tdSelStart)
\t\t\tswitch tdClass {
\t\t\tcase 1:
\t\t\t\tself.tdNoCap += tdWait
\t\t\tcase 2:
\t\t\t\tself.tdOtherNil += tdWait
\t\t\tdefault:
\t\t\t\tself.tdStarve += tdWait
\t\t\t}
\t\t\tself.tdLoops += 1
\t\t\tif self.tdLast.IsZero() {
\t\t\t\tself.tdLast = tdNow
\t\t\t} else if tdWall := tdNow.Sub(self.tdLast); time.Second <= tdWall {
\t\t\t\tif 200 < self.tdLoops {
\t\t\t\t\tpct := func(d time.Duration) float64 { return 100 * float64(d) / float64(tdWall) }
\t\t\t\t\twork := tdWall - self.tdNoCap - self.tdOtherNil - self.tdStarve
\t\t\t\t\tself.log.Infof("TDIAG dest=%v loops=%d noCap=%.1f%% otherNil=%.1f%% starve=%.1f%% work=%.1f%% rqAvg=%dK rqMax=%dK rqCap=%dK resends=%d selective=%d\\n", self.destination, self.tdLoops, pct(self.tdNoCap), pct(self.tdOtherNil), pct(self.tdStarve), pct(work), self.tdRqSum/max(1, int64(self.tdRqN))/1024, self.tdRqMax/1024, int64(self.sendBufferSettings.ResendQueueMaxByteCount)/1024, self.tdResend, self.tdSelResend)
\t\t\t\t}
\t\t\t\tself.tdLast = tdNow
\t\t\t\tself.tdNoCap, self.tdOtherNil, self.tdStarve, self.tdLoops, self.tdResend, self.tdSelResend = 0, 0, 0, 0, 0, 0
\t\t\t\tself.tdRqSum, self.tdRqN, self.tdRqMax = 0, 0, 0
\t\t\t}
\t\t}
""" + end, 1)
rec = "\t\t\t\tself.client.recordSendRecovery(recoveryKind, resendErr)\n"
assert s.count(rec) == 1, 'recovery anchor'
s = s.replace(rec, "\t\t\t\tif tdiagEnabled {\n\t\t\t\t\tself.tdResend += 1\n\t\t\t\t\tif recoveryKind != sendRecoveryNone {\n\t\t\t\t\t\tself.tdSelResend += 1\n\t\t\t\t\t}\n\t\t\t\t}\n" + rec, 1)
typ = "type SendSequence struct {\n"
assert s.count(typ) == 1
s = s.replace(typ, typ + "\ttdLast                              time.Time\n\ttdNoCap, tdOtherNil, tdStarve       time.Duration\n\ttdLoops, tdRqN, tdResend, tdSelResend int\n\ttdRqSum, tdRqMax                    int64\n", 1)
ack = "\t\tAckCompressTimeout:  10 * time.Millisecond,\n"
if s.count(ack) != 1:
    ack = "\t\tAckCompressTimeout: 10 * time.Millisecond,\n"
assert s.count(ack) == 1, 'ack anchor'
s = s.replace(ack, "\t\tAckCompressTimeout: envAckCompressTimeout(10 * time.Millisecond),\n", 1)
ackb = "\t\tAckCompressByteCount: kib(128),\n"
if s.count(ackb) == 1:
    s = s.replace(ackb, "\t\tAckCompressByteCount: envAckCompressByteCount(kib(128)),\n", 1)
t.write_text(s)
(root / 'zz_tdiag_env.go').write_text('''package connect

// DIAGNOSTIC ONLY (never committed)

import (
	"os"
	"strconv"
	"time"
)

var tdiagEnabled = os.Getenv("URNETWORK_TDIAG") == "1"

func envAckCompressByteCount(def ByteCount) ByteCount {
	if n, err := strconv.Atoi(os.Getenv("URNETWORK_ACK_COMPRESS_BYTES")); err == nil && 0 <= n {
		return ByteCount(n)
	}
	return def
}

func envAckCompressTimeout(def time.Duration) time.Duration {
	v := os.Getenv("URNETWORK_ACK_COMPRESS_US")
	if v == "off" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil && 0 < n {
		return time.Duration(n) * time.Microsecond
	}
	return def
}
''')
print("tdiag patched")

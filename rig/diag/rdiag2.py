#!/usr/bin/env python3
"""DIAGNOSTIC ONLY (never committed). URNETWORK_TDIAG=1 on the RECEIVER: per ReceiveSequence, per 1 s:
head / past (already delivered: spurious resend) / future (gap arrival, by distance) / dupQueued, and
blocked = wall time with out-of-order items queued (cumulative head stuck)."""
import sys, pathlib
t = pathlib.Path(sys.argv[1]) / 'transfer.go'; s = t.read_text()
assert 'RDIAG' not in s
dup = "\tif removedItem := self.receiveQueue.RemoveBySequenceNumber(sequenceNumber); removedItem != nil {\n"
assert s.count(dup) == 1, 'dup anchor'
s = s.replace(dup, dup + "\t\tif tdiagEnabled {\n\t\t\tself.rdDupQueued += 1\n\t\t}\n", 1)
anchor = "\tif sequenceNumber <= self.nextSequenceNumber {\n\t\tif self.nextSequenceNumber == sequenceNumber {\n\t\t\t// this item is the head of sequence\n"
assert s.count(anchor) == 1, 'order anchor %d' % s.count(anchor)
s = s.replace(anchor, """\tif tdiagEnabled {
\t\trdNow := time.Now()
\t\tif queued, _ := self.receiveQueue.QueueSize(); 0 < queued && !self.rdLastCall.IsZero() {
\t\t\tself.rdBlocked += rdNow.Sub(self.rdLastCall)
\t\t}
\t\tself.rdLastCall = rdNow
\t\tswitch {
\t\tcase sequenceNumber == self.nextSequenceNumber:
\t\t\tself.rdHead += 1
\t\tcase sequenceNumber < self.nextSequenceNumber:
\t\t\tself.rdPast += 1
\t\tdefault:
\t\t\td := sequenceNumber - self.nextSequenceNumber
\t\t\tswitch {
\t\t\tcase d <= 1:
\t\t\t\tself.rdFut1 += 1
\t\t\tcase d <= 8:
\t\t\t\tself.rdFut8 += 1
\t\t\tcase d <= 64:
\t\t\t\tself.rdFut64 += 1
\t\t\tdefault:
\t\t\t\tself.rdFutBig += 1
\t\t\t}
\t\t}
\t\tif self.rdStart.IsZero() {
\t\t\tself.rdStart = rdNow
\t\t} else if rdWall := rdNow.Sub(self.rdStart); time.Second <= rdWall {
\t\t\tif 1000 < self.rdHead {
\t\t\t\tself.log.Infof("RDIAG src=%v head=%d past=%d fut1=%d fut2_8=%d fut9_64=%d futBig=%d dupQueued=%d blocked=%.1f%%\\n", self.source.SourceId, self.rdHead, self.rdPast, self.rdFut1, self.rdFut8, self.rdFut64, self.rdFutBig, self.rdDupQueued, 100*float64(self.rdBlocked)/float64(rdWall))
\t\t\t}
\t\t\tself.rdStart = rdNow
\t\t\tself.rdHead, self.rdPast, self.rdFut1, self.rdFut8, self.rdFut64, self.rdFutBig, self.rdDupQueued, self.rdBlocked = 0, 0, 0, 0, 0, 0, 0, 0
\t\t}
\t}
""" + anchor, 1)
typ = "type ReceiveSequence struct {\n"
assert s.count(typ) == 1
s = s.replace(typ, typ + "\trdStart, rdLastCall time.Time\n\trdBlocked time.Duration\n\trdHead, rdPast, rdFut1, rdFut8, rdFut64, rdFutBig, rdDupQueued int\n", 1)
t.write_text(s); print("rdiag2 patched")

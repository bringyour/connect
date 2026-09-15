#!/usr/bin/env python3
"""DIAGNOSTIC ONLY (never committed), on top of tdiag.py: TDIAG also reports per-second Transfer writes handed to the
route writer (initial + resend) and write errors, so provider writes can be compared with client arrivals (RDIAG)."""
import sys, pathlib
t = pathlib.Path(sys.argv[1]) / 'transfer.go'; s = t.read_text()
assert 'tdWrites' not in s and 'TDIAG dest=' in s, 'needs tdiag.py first'
new_site = "\tif err == nil {\n\t\titem.transportWriteObserved = true\n\t\titem.acks.observeTransportWrite(writeDisposition.transportType)\n\t}\n"
assert s.count(new_site) == 1, 'initial write anchor'
s = s.replace(new_site, "\tif tdiagEnabled {\n\t\tself.tdWrites += 1\n\t\tif err != nil {\n\t\t\tself.tdWriteErrs += 1\n\t\t}\n\t}\n" + new_site, 1)
res_site = "\t\t\t\tif resendErr == nil {\n\t\t\t\t\tif !item.transportWriteObserved {"
assert s.count(res_site) == 1, 'resend anchor'
s = s.replace(res_site, "\t\t\t\tif tdiagEnabled {\n\t\t\t\t\tself.tdWrites += 1\n\t\t\t\t\tif resendErr != nil {\n\t\t\t\t\t\tself.tdWriteErrs += 1\n\t\t\t\t\t}\n\t\t\t\t}\n" + res_site, 1)
s = s.replace("\ttdLoops, tdRqN, tdResend, tdSelResend int\n", "\ttdLoops, tdRqN, tdResend, tdSelResend int\n\ttdWrites, tdWriteErrs int\n", 1)
old_fmt = 'rqCap=%dK resends=%d selective=%d\\n", self.destination'
assert s.count(old_fmt) == 1, 'fmt anchor'
s = s.replace(old_fmt, 'rqCap=%dK resends=%d selective=%d writes=%d writeErrs=%d\\n", self.destination', 1)
s = s.replace('self.tdResend, self.tdSelResend)', 'self.tdResend, self.tdSelResend, self.tdWrites, self.tdWriteErrs)', 1)
old_reset = 'self.tdNoCap, self.tdOtherNil, self.tdStarve, self.tdLoops, self.tdResend, self.tdSelResend = 0, 0, 0, 0, 0, 0'
assert s.count(old_reset) == 1, 'reset anchor'
s = s.replace(old_reset, old_reset + '\n\t\t\t\tself.tdWrites, self.tdWriteErrs = 0, 0', 1)
t.write_text(s); print('wcount patched')

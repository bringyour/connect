#!/usr/bin/env python3
"""DIAGNOSTIC ONLY: with URNETWORK_TDIAG=1 log per-second process totals of H1 websocket messages written (into the
write batch), batch flush errors, and messages read and delivered (needs tdiag.py's tdiagEnabled)."""
import sys, pathlib
p = pathlib.Path(sys.argv[1]) / 'transport.go'; s = p.read_text()
assert 'xdiagH1Writes' not in s
old_w = """					if err := writeMessage(message); err != nil {
						return err
					}
					writeCounter.Add(1)
					return nil
				}

				writeBatchConn, _ :="""
assert s.count(old_w) == 1, 'write anchor'
s = s.replace(old_w, old_w.replace("\t\t\t\t\tif err := writeMessage(message); err != nil {", "\t\t\t\t\tmessageLen := len(message)\n\t\t\t\t\tif err := writeMessage(message); err != nil {").replace("writeCounter.Add(1)\n", "writeCounter.Add(1)\n\t\t\t\t\txdiagH1Writes.Add(1)\n\t\t\t\t\tif 600 < messageLen {\n\t\t\t\t\t\txdiagH1WritesLarge.Add(1)\n\t\t\t\t\t}\n"), 1)
old_f = """						self.log.Infof("[ts]%s-> batch flush error = %s\\n", clientId, err)"""
assert s.count(old_f) == 1, 'flush anchor'
s = s.replace(old_f, old_f + "\n\t\t\t\t\t\txdiagH1FlushErrors.Add(1)", 1)
old_r = """						if delivered {
							readCounter.Add(1)
						}"""
assert s.count(old_r) >= 1, 'read anchor'
s = s.replace(old_r, """						if delivered {
							readCounter.Add(1)
							xdiagH1Reads.Add(1)
							if 600 < len(message) {
								xdiagH1ReadsLarge.Add(1)
							}
						}""", 1)
p.write_text(s)
(pathlib.Path(sys.argv[1]) / 'zz_xdiag.go').write_text('''package connect

// DIAGNOSTIC ONLY (never committed)

import (
	"sync/atomic"
	"time"
)

var xdiagH1Writes, xdiagH1Reads, xdiagH1FlushErrors, xdiagH1WritesLarge, xdiagH1ReadsLarge atomic.Uint64

func init() {
	if !tdiagEnabled {
		return
	}
	go func() {
		log := DefaultLogger()
		var lw, lr, lf, lwl, lrl uint64
		for {
			time.Sleep(time.Second)
			w, r, f, wl, rl := xdiagH1Writes.Load(), xdiagH1Reads.Load(), xdiagH1FlushErrors.Load(), xdiagH1WritesLarge.Load(), xdiagH1ReadsLarge.Load()
			if w != lw || r != lr {
				log.Infof("XDIAG h1 writes=%d reads=%d flushErrors=%d writesLarge=%d readsLarge=%d unix=%d\\n", w-lw, r-lr, f-lf, wl-lwl, rl-lrl, time.Now().Unix())
			}
			lw, lr, lf, lwl, lrl = w, r, f, wl, rl
		}
	}()
}
''')
print('xdiag patched')

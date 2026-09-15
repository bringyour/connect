#!/usr/bin/env python3
"""DIAGNOSTIC ONLY: with URNETWORK_TDIAG=1 the provider logs its congestion drop snapshot every 2 s when it changes."""
import sys, pathlib
p = pathlib.Path(sys.argv[1]) / 'ip.go'; s = p.read_text()
assert 'CDIAG' not in s
old = "func (self *RemoteUserNatProvider) runPacketStats() {\n\tvar lastPacketStats *PacketStats\n"
assert s.count(old) == 1
s = s.replace(old, old + """\tif tdiagEnabled {
\t\tgo func() {
\t\t\tvar last ProviderCongestionDrops
\t\t\tfor {
\t\t\t\tselect {
\t\t\t\tcase <-self.ctx.Done():
\t\t\t\t\treturn
\t\t\t\tcase <-time.After(2 * time.Second):
\t\t\t\t}
\t\t\t\tcur := self.congestionDrops.snapshot()
\t\t\t\tif cur != last {
\t\t\t\t\tself.client.log.Infof("CDIAG ingressNat=%d returnQueue=%d returnSend=%d (+%d +%d +%d)\\n", cur.IngressNatPacketCount, cur.ReturnQueuePacketCount, cur.ReturnSendPacketCount, cur.IngressNatPacketCount-last.IngressNatPacketCount, cur.ReturnQueuePacketCount-last.ReturnQueuePacketCount, cur.ReturnSendPacketCount-last.ReturnSendPacketCount)
\t\t\t\t\tlast = cur
\t\t\t\t}
\t\t\t}
\t\t}()
\t}
""", 1)
p.write_text(s); print("cdiag patched")

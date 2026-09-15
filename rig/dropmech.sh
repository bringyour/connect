#!/usr/bin/env bash
# dropmech.sh : mechanism at 0 / 0.5% / 2% injected loss; provider tdiag, client urtun-x5 diag; 2 rotations
S=$(cd $(dirname $0); pwd); C=root@$CLIENT_HOST
for r in 1 2; do
  if [ $r = 1 ]; then o="0 5000 20000"; else o="20000 5000 0"; fi
  for ppm in $o; do
    L=dm-$ppm-r$r
    PB=dropdiag CLIENT_MODE=tun TUN_BIN=/tmp/urtun-x5 CE="URNETWORK_TDIAG=1" DUR=30 bash $S/tdone.sh $L 8 synth "URNETWORK_DIAG_DROP_PPM=$ppm" | grep -o "bin=[0-9a-f]* goodput=[0-9]*\|md5=[0-9a-f]*" | tr '\n' ' '; echo " <- $L"
    ssh -o BatchMode=yes $C "grep 'RDIAG\|XDIAG' /tmp/tm-$L.log" > $S/cl-$L.txt
  done
done
echo DROPMECH DONE

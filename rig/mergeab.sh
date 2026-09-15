#!/usr/bin/env bash
# upstream group merge A/B: M0 vs M1 (URNETWORK_DIAG_GROUP_MERGE=1). SG + TDIAG on both. provider b7, client urtun-x14. f8 n=4, f1 n=3
S=$(cd $(dirname $0); pwd); C=root@$CLIENT_HOST
BASE="URNETWORK_TDIAG=1 URNETWORK_DIAG_GAP_ACK_NOW=1 URNETWORK_DIAG_SACK_SORT=1"
echo "MERGEAB START $(date -u +%T)"
for r in 1 2 3 4; do
  if [ $((r % 2)) = 1 ]; then o="M1 M0"; else o="M0 M1"; fi
  for f in 8 1; do
    [ $f = 1 ] && [ $r = 4 ] && continue
    for a in $o; do
      L=mg$f-$a-r$r; grep -q "^$L " $S/mergeab.out 2>/dev/null && continue
      ce="$BASE"; [ $a = M1 ] && ce="$ce URNETWORK_DIAG_GROUP_MERGE=1"
      out=$(CLIENT_MODE=tun TUN_BIN=/tmp/urtun-x14 bash $S/ceil2.sh $L b7 $f 30 synth "$ce" "")
      ws=$(ssh -o BatchMode=yes $C "python3 - /tmp/tm-$L.log" <<'PY'
import re,sys,statistics as st
t=open(sys.argv[1]).read()
X=[tuple(map(int,m.groups())) for m in re.finditer(r'XDIAG h1 writes=(\d+) reads=(\d+) flushErrors=\d+ writesLarge=\d+ readsLarge=(\d+)',t)]
X=[x for x in X if x[2]>3000][2:-2]
R=[tuple(map(int,m.groups())) for m in re.finditer(r'TDIAG dest=\S+ .*?resends=(\d+) selective=\d+ writes=(\d+)',t)]
R=[x for x in R if x[1]>500][2:-2]
M=[tuple(map(int,m.groups())) for m in re.finditer(r'mergeItems=(\d+) mergePacks=(\d+)',t)][3:-3]
md=lambda L,i: int(st.median([x[i] for x in L])) if L else -1
print("upSeqItems/s=%d upWsMsgs/s=%d dnData/s=%d mergeItems/s=%d mergePacks/s=%d"%(md(R,1),md(X,0),md(X,2),md(M,0),md(M,1)))
PY
)
      st=$(ssh -o BatchMode=yes $C "grep -c 'evaluation ping timeout' /tmp/tm-$L.log")
      echo "$L $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|client\[cores=[0-9.]*\|relay\[cores=[0-9.]*\|md5=[0-9a-f]*' | tr '\n' ' ') $ws stall=$st"
    done
  done
done
echo "MERGEAB DONE $(date -u +%T)"

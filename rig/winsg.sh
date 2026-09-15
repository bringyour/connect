#!/usr/bin/env bash
# window sweep on top of the loss-recovery fix. provider ldiag2 (URNETWORK_WIN_MIB), client urtun-x10 (RWIN 32 all arms)
# arms f8: w2sg w4sg w8sg w8off ; f1: w2sg w4sg w8sg ; n=3 rotated
S=$(cd $(dirname $0); pwd); C=root@$CLIENT_HOST
SG="URNETWORK_DIAG_GAP_ACK_NOW=1 URNETWORK_DIAG_SACK_SORT=1"
echo "WINSG START $(date -u +%T)"
arms8=(w2sg w4sg w8sg w8off); arms1=(w2sg w4sg w8sg)
for r in 1 2 3; do
  for k in 0 1 2 3; do
    a=${arms8[$(( (k + r) % 4 ))]}; w=${a:1:1}; ce="URNETWORK_TDIAG=1 URNETWORK_RWIN_MIB=32"; [ "${a:2}" = sg ] && ce="$ce $SG"
    pe=""; [ $w != 2 ] && pe="URNETWORK_WIN_MIB=$w"
    L=ws8-$a-r$r; out=$(PB=ldiag2 CLIENT_MODE=tun TUN_BIN=/tmp/urtun-x10 CE="$ce" DUR=30 bash $S/tdone.sh $L 8 synth "$pe")
    ssh -o BatchMode=yes $C "grep 'LDIAG\|RDIAG' /tmp/tm-$L.log" > $S/cl-$L.txt
    echo "$L $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|md5=[0-9a-f]*' | tr '\n' ' ') stall=$(ssh -o BatchMode=yes $C "grep -c 'evaluation ping timeout' /tmp/tm-$L.log")"
  done
  for k in 0 1 2; do
    a=${arms1[$(( (k + r) % 3 ))]}; w=${a:1:1}; ce="URNETWORK_TDIAG=1 URNETWORK_RWIN_MIB=32 $SG"
    pe=""; [ $w != 2 ] && pe="URNETWORK_WIN_MIB=$w"
    L=ws1-$a-r$r; out=$(PB=ldiag2 CLIENT_MODE=tun TUN_BIN=/tmp/urtun-x10 CE="$ce" DUR=30 bash $S/tdone.sh $L 1 synth "$pe")
    ssh -o BatchMode=yes $C "grep 'LDIAG\|RDIAG' /tmp/tm-$L.log" > $S/cl-$L.txt
    echo "$L $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|md5=[0-9a-f]*' | tr '\n' ' ') stall=$(ssh -o BatchMode=yes $C "grep -c 'evaluation ping timeout' /tmp/tm-$L.log")"
  done
done
echo "WINSG DONE $(date -u +%T)"

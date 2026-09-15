#!/usr/bin/env bash
# SG confirmation, same-DC TUN: f8 n=6 and f1 n=4, alternating ctl/SG
S=$(cd $(dirname $0); pwd); C=root@$CLIENT_HOST
echo "SGCONF START $(date -u +%T)"
run() { # label flows arm
  ce="URNETWORK_TDIAG=1"; [ $3 = SG ] && ce="$ce URNETWORK_DIAG_GAP_ACK_NOW=1 URNETWORK_DIAG_SACK_SORT=1"
  out=$(PB=ldiag CLIENT_MODE=tun TUN_BIN=/tmp/urtun-x10 CE="$ce" DUR=30 bash $S/tdone.sh $1 $2 synth "")
  ssh -o BatchMode=yes $C "grep 'LDIAG\|RDIAG' /tmp/tm-$1.log" > $S/cl-$1.txt
  echo "$1 $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|md5=[0-9a-f]*\|z0=[0-9]* z1=[0-9]*' | tr '\n' ' ')"
}
for r in 1 2 3 4 5 6; do
  if [ $((r % 2)) = 1 ]; then o="SG ctl"; else o="ctl SG"; fi
  for a in $o; do run sg8-$a-r$r 8 $a; done
  if [ $r -le 4 ]; then for a in $o; do run sg1-$a-r$r 1 $a; done; fi
done
echo "SGCONF DONE $(date -u +%T)"

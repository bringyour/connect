#!/usr/bin/env bash
# arms ctl | G (gap-triggered ack) | SG (sorted sacks + G) | SR2G (sorted + release 2 MiB + rwin 32 + G); TUN f8 30 s; n=4
S=$(cd $(dirname $0); pwd); C=root@$CLIENT_HOST
arms=(ctl G SG SR2G)
echo "GAPAB START $(date -u +%T)"
for r in 1 2 3 4; do
  for k in 0 1 2 3; do
    a=${arms[$(( (k + r) % 4 ))]}
    ce="URNETWORK_TDIAG=1"; pe=""
    case $a in
      G) ce="$ce URNETWORK_DIAG_GAP_ACK_NOW=1";;
      SG) ce="$ce URNETWORK_DIAG_GAP_ACK_NOW=1 URNETWORK_DIAG_SACK_SORT=1";;
      SR2G) ce="$ce URNETWORK_DIAG_GAP_ACK_NOW=1 URNETWORK_DIAG_SACK_SORT=1 URNETWORK_RWIN_MIB=32"; pe="URNETWORK_DIAG_SACK_RELEASE_KIB=2048";;
    esac
    L=ga-$a-r$r
    out=$(PB=ldiag CLIENT_MODE=tun TUN_BIN=/tmp/urtun-x10 CE="$ce" DUR=30 bash $S/tdone.sh $L 8 synth "$pe")
    ssh -o BatchMode=yes $C "grep 'LDIAG\|RDIAG\|XDIAG' /tmp/tm-$L.log" > $S/cl-$L.txt
    echo "$L $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|md5=[0-9a-f]*\|z0=[0-9]* z1=[0-9]*' | tr '\n' ' ')"
  done
done
echo "GAPAB DONE $(date -u +%T)"

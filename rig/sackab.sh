#!/usr/bin/env bash
# T19 fix candidates, TUN f8 synth 30 s. arms: ctl | S (client sorted sacks) | R (provider releases sacked bytes <=512K)
# | SR | SR2 (release <=2048K, client receive queue 32 MiB). provider sackdiag, client urtun-x6. n=4 rotations.
S=$(cd $(dirname $0); pwd); C=root@$CLIENT_HOST
arms=(ctl S R SR SR2)
echo "SACKAB START $(date -u +%T)"
for r in 1 2 3 4; do
  for k in 0 1 2 3 4; do
    a=${arms[$(( (k + r) % 5 ))]}
    ce="URNETWORK_TDIAG=1"; pe=""
    case $a in
      S) ce="$ce URNETWORK_DIAG_SACK_SORT=1";;
      R) pe="URNETWORK_DIAG_SACK_RELEASE_KIB=512";;
      SR) ce="$ce URNETWORK_DIAG_SACK_SORT=1"; pe="URNETWORK_DIAG_SACK_RELEASE_KIB=512";;
      SR2) ce="$ce URNETWORK_DIAG_SACK_SORT=1 URNETWORK_RWIN_MIB=32"; pe="URNETWORK_DIAG_SACK_RELEASE_KIB=2048";;
    esac
    L=sk-$a-r$r
    out=$(PB=sackdiag CLIENT_MODE=tun TUN_BIN=/tmp/urtun-x6 CE="$ce" DUR=30 bash $S/tdone.sh $L 8 synth "$pe")
    ssh -o BatchMode=yes $C "grep 'RDIAG\|XDIAG' /tmp/tm-$L.log" > $S/cl-$L.txt
    echo "$L $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|md5=[0-9a-f]*\|z0=[0-9]* z1=[0-9]*' | tr '\n' ' ')"
  done
done
echo "SACKAB DONE $(date -u +%T)"

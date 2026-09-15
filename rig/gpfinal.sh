#!/usr/bin/env bash
# production code A/B: clean provider b-keys-fix, clients urtun-gp-base vs urtun-gp-fix; TUN f8 n=6, f1 n=4; stall grep
S=$(cd $(dirname $0); pwd); C=root@$CLIENT_HOST
run() { # label flows arm
  out=$(CLIENT_MODE=tun TUN_BIN=/tmp/urtun-gp-$3 bash $S/ceil2.sh $1 b-keys-fix $2 30 synth "" "")
  st=$(ssh -o BatchMode=yes $C "grep -c 'evaluation ping timeout\|window_stall' /tmp/tm-$1.log")
  echo "$1 $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|provider\[[^]]*\]\|md5=[0-9a-f]*\|z0=[0-9]* z1=[0-9]*' | tr '\n' ' ') stall=$st"
}
echo "GPFINAL START $(date -u +%T)"
for r in 1 2 3 4 5 6; do
  if [ $((r % 2)) = 1 ]; then o="fix base"; else o="base fix"; fi
  for a in $o; do run gp8-$a-r$r 8 $a; done
  if [ $r -le 4 ]; then for a in $o; do run gp1-$a-r$r 1 $a; done; fi
done
echo "GPFINAL DONE $(date -u +%T)"

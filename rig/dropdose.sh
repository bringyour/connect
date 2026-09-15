#!/usr/bin/env bash
# dropdose.sh N : injected post-route message loss 0 / 0.5% / 2%, TUN f8 synth 30 s, rotated order
S=$(cd $(dirname $0); pwd)
echo "DROPDOSE START $(date -u +%T)"
for r in $(seq 1 ${1:-4}); do
  case $((r % 3)) in 1) o="0 5000 20000";; 2) o="5000 20000 0";; 0) o="20000 0 5000";; esac
  for ppm in $o; do
    CLIENT_MODE=tun TUN_BIN=/tmp/urtun-clean-new bash $S/ceil2.sh dd-$ppm-r$r drop 8 30 synth "" "URNETWORK_DIAG_DROP_PPM=$ppm" | grep -o "dd-[0-9]*-r[0-9]*\|bin=[0-9a-f]* goodput=[0-9]*\|provider\[[^]]*\]\|relay\[[^]]*\]\|md5=[0-9a-f]* z0=[0-9]* z1=[0-9]*" | tr '\n' ' '; echo
  done
done
echo "DROPDOSE DONE $(date -u +%T)"

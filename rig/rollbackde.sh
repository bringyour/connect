#!/usr/bin/env bash
# Germany rollback: MGr vs MGc (mg2 provider + urtun-mg2, budget 384), f1 and f8, n=3 alternating
S=$(cd $(dirname $0); pwd); DE=root@$FAR_CLIENT_HOST
until grep -q "ROLLBACK DONE" $S/rollback.out 2>/dev/null; do sleep 30; done
echo "RBDE START $(date -u +%T)"
for r in 1 2 3; do
  if [ $((r % 2)) = 1 ]; then o="MGr MGc"; else o="MGc MGr"; fi
  for f in 1 8; do for a in $o; do
    ce="HARNESS_MEMORY_BUDGET_MIB=384"; pe=""
    [ $a = MGc ] && { ce="$ce URNETWORK_WINDOW_SIZING=constant"; pe="URNETWORK_WINDOW_SIZING=constant"; }
    L=rd$f-$a-r$r; grep -q "^$L " $S/rollbackde.out 2>/dev/null && continue
    out=$(CLIENT=$DE CLIENT_MODE=tun TUN_BIN=/tmp/urtun-mg2 bash $S/ceil2.sh $L mg2 $f 30 synth "$ce" "$pe")
    st=$(ssh -o BatchMode=yes $DE "grep -c 'evaluation ping timeout' /tmp/tm-$L.log")
    echo "$L $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|md5=[0-9a-f]*' | tr '\n' ' ') stall=$st"
  done; done
done
echo "RBDE DONE $(date -u +%T)"

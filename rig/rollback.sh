#!/usr/bin/env bash
# window-rule rollback test, same-DC TUN: arms UPr (up2 rule on), UPc (up2 constant both ends), MGr, MGc; budget 384 on clients; f8, f1; n=3
S=$(cd $(dirname $0); pwd); C=root@$CLIENT_HOST
until grep -q "UPMATRIX de DONE" $S/upmatrix-de.out 2>/dev/null; do sleep 30; done
arms=(UPr UPc MGr MGc)
echo "ROLLBACK START $(date -u +%T)"
for r in 1 2 3; do
  for f in 8 1; do
    for k in 0 1 2 3; do
      a=${arms[$(( (k + r) % 4 ))]}
      case $a in
        UPr) pb=up2; bin=/tmp/urtun-up2; ce="HARNESS_MEMORY_BUDGET_MIB=384"; pe="";;
        UPc) pb=up2; bin=/tmp/urtun-up2; ce="HARNESS_MEMORY_BUDGET_MIB=384 URNETWORK_WINDOW_SIZING=constant"; pe="URNETWORK_WINDOW_SIZING=constant";;
        MGr) pb=mg2; bin=/tmp/urtun-mg2; ce="HARNESS_MEMORY_BUDGET_MIB=384"; pe="";;
        MGc) pb=mg2; bin=/tmp/urtun-mg2; ce="HARNESS_MEMORY_BUDGET_MIB=384 URNETWORK_WINDOW_SIZING=constant"; pe="URNETWORK_WINDOW_SIZING=constant";;
      esac
      L=rb$f-$a-r$r; grep -q "^$L " $S/rollback.out 2>/dev/null && continue
      out=$(CLIENT_MODE=tun TUN_BIN=$bin bash $S/ceil2.sh $L $pb $f 30 synth "$ce" "$pe")
      st=$(ssh -o BatchMode=yes $C "grep -c 'evaluation ping timeout' /tmp/tm-$L.log")
      echo "$L $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|client\[cores=[0-9.]*\|provider\[cores=[0-9.]*\|md5=[0-9a-f]*' | tr '\n' ' ') stall=$st"
    done
  done
done
echo "ROLLBACK DONE $(date -u +%T)"

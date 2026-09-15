#!/usr/bin/env bash
# upstream window-rule review matrix. arms: BETA (b7 + urtun-gp-fix), UP (up + urtun-up, budget 384), UPnb (up + urtun-up, no budget),
# MG (mg + urtun-mg, budget 384). CLIENT host/env via args: $1 = label prefix, $2 = client ssh, $3 = flows list, $4 = reps
S=$(cd $(dirname $0); pwd); PFX=$1; CL=$2; FLOWS=$3; N=$4
arms=(BETA UP PA PB)
echo "PRMATRIX $PFX START $(date -u +%T)"
for r in $(seq 1 $N); do
  for f in $FLOWS; do
    for k in 0 1 2 3; do
      a=${arms[$(( (k + r) % 4 ))]}
      case $a in
        BETA) pb=b7; bin=/tmp/urtun-gp-fix; ce="";;
        UP) pb=up; bin=/tmp/urtun-up; ce="HARNESS_MEMORY_BUDGET_MIB=384";;
        PA) pb=pa; bin=/tmp/urtun-pa; ce="HARNESS_MEMORY_BUDGET_MIB=384";;
        PB) pb=pb; bin=/tmp/urtun-pb; ce="HARNESS_MEMORY_BUDGET_MIB=384";;
      esac
      L=$PFX$f-$a-r$r
      grep -q "^$L " $S/prmatrix-$PFX.out 2>/dev/null && continue
      out=$(CLIENT=$CL CLIENT_MODE=tun TUN_BIN=$bin bash $S/ceil2.sh $L $pb $f 30 synth "$ce" "")
      st=$(ssh -o BatchMode=yes $CL "grep -c 'evaluation ping timeout' /tmp/tm-$L.log")
      echo "$L $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|client\[cores=[0-9.]*\|provider\[cores=[0-9.]*\|relay\[cores=[0-9.]*\|md5=[0-9a-f]*' | tr '\n' ' ') stall=$st"
    done
  done
done
echo "PRMATRIX $PFX DONE $(date -u +%T)"

#!/usr/bin/env bash
# correlate client kernel TCP backlog drops with single-flow stalls: PA arm, f1, n=8
S=$(cd $(dirname $0); pwd); C=root@$CLIENT_HOST
nst() { ssh -o BatchMode=yes $C "nstat -az | awk '/TcpExtTCPBacklogDrop|TcpExtPruneCalled|TcpExtTCPOFOQueue/ {printf \"%s=%s \", \$1, \$2}'"; }
echo "BLDROP START $(date -u +%T)"
for r in $(seq 1 8); do
  L=bd1-r$r; a=$(nst)
  out=$(CLIENT_MODE=tun TUN_BIN=/tmp/urtun-pa bash $S/ceil2.sh $L pa 1 30 synth "HARNESS_MEMORY_BUDGET_MIB=384" "")
  b=$(nst)
  echo "$L $(echo "$out" | grep -o 'goodput=[0-9]*') before[$a] after[$b]"
done
echo "BLDROP DONE $(date -u +%T)"

#!/usr/bin/env bash
# loop f1 runs (provider b7 clean, client urtun-gp-fix) with stall watchers on both ends until a goroutine dump is captured (max 14)
S=$(cd $(dirname $0); pwd); P=root@$PROVIDER_HOST; C=root@$CLIENT_HOST
echo "CATCH START $(date -u +%T)"
for r in $(seq 1 14); do
  L=cs1-r$r
  ( sleep 27; ssh -o BatchMode=yes $P "rm -f /tmp/sw-$L.*; bash /root/stallwatch.sh $L eth0 tx 6060 52" ) &
  W1=$!
  ( sleep 27; ssh -o BatchMode=yes $C "rm -f /tmp/sw-$L.*; bash /root/stallwatch.sh $L eth0 rx 6061 52" ) &
  W2=$!
  out=$(CLIENT_MODE=tun TUN_BIN=/tmp/urtun-gp-fix bash $S/ceil2.sh $L b7 1 30 synth "" "")
  wait $W1 $W2
  pd=$(ssh -o BatchMode=yes $P "grep -c DUMPED /tmp/sw-$L.rate 2>/dev/null; wc -c < /tmp/sw-$L.goroutines 2>/dev/null" | tr '\n' ' ')
  cd_=$(ssh -o BatchMode=yes $C "grep -c DUMPED /tmp/sw-$L.rate 2>/dev/null; wc -c < /tmp/sw-$L.goroutines 2>/dev/null" | tr '\n' ' ')
  echo "$L $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|md5=[0-9a-f]*' | tr '\n' ' ') provDump=[$pd] cliDump=[$cd_]"
  g=$(echo "$out" | grep -o 'goodput=[0-9]*' | cut -d= -f2)
  if echo "$pd" | grep -q '^1 ' && [ "${g:-999}" -lt 600 ]; then
    scp -o BatchMode=yes -q $P:/tmp/sw-$L.goroutines $S/prov-gr-$L.txt; scp -o BatchMode=yes -q $P:/tmp/sw-$L.rate $S/prov-rate-$L.txt
    scp -o BatchMode=yes -q $C:/tmp/sw-$L.goroutines $S/cli-gr-$L.txt 2>/dev/null; scp -o BatchMode=yes -q $C:/tmp/sw-$L.rate $S/cli-rate-$L.txt 2>/dev/null
    echo "CAUGHT $L"; break
  fi
done
echo "CATCH DONE $(date -u +%T)"

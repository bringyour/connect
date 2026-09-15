#!/usr/bin/env bash
# ACK coalescing A/B: D (direct read), K (read channel), C (channel + coalesce). SG on, provider b7, client urtun-x11
S=$(cd $(dirname $0); pwd); C=root@$CLIENT_HOST
SG="URNETWORK_DIAG_GAP_ACK_NOW=1 URNETWORK_DIAG_SACK_SORT=1"
arms=(D K C)
echo "ACOAB RESUME $(date -u +%T)"
for r in 1 2 3 4; do
  for f in 8 1; do
    [ $f = 1 ] && [ $r = 4 ] && continue
    for k in 0 1 2; do
      a=${arms[$(( (k + r) % 3 ))]}; ce="$SG"
      [ $a = K ] && ce="$ce TUN_READ_CHAN=1"; [ $a = C ] && ce="$ce TUN_READ_CHAN=1 TUN_ACK_COALESCE=1"
      L=ac$f-$a-r$r
      grep -q "^$L " $S/acoab.out 2>/dev/null && continue
      out=$(CLIENT_MODE=tun TUN_BIN=/tmp/urtun-x11 bash $S/ceil2.sh $L b7 $f 30 synth "$ce" "")
      dr=$(ssh -o BatchMode=yes $C "grep -o 'ACKCO in=[0-9]* dropped=[0-9]*' /tmp/tm-$L.log | sed -n '10,20p' | awk '{split(\$2,a,\"=\");split(\$3,b,\"=\"); i+=a[2]; d+=b[2]} END{if(i) printf \"in/s=%d drop%%=%.0f\", i/NR, 100*d/i}'")
      st=$(ssh -o BatchMode=yes $C "grep -c 'evaluation ping timeout' /tmp/tm-$L.log")
      echo "$L $(echo "$out" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*\|client\[[^]]*\]\|provider\[[^]]*\]\|relay\[[^]]*\]\|md5=[0-9a-f]*' | tr '\n' ' ') $dr stall=$st"
    done
  done
done
echo "ACOAB DONE $(date -u +%T)"

#!/usr/bin/env bash
# tmeas.sh LABEL FLOWS DUR TARGET(synth|cdn) [CLIENT_ENV]   (client host; kernel TUN client, curl on host TCP)
L=$1; F=$2; D=$3; T=$4; E=${5:-}
BIN=${TUN_BIN:-/tmp/urtun-td}; DEV=ur0
PROV="${PROVIDER_ID:?set PROVIDER_ID}"; JWT="$(cat "${JWT_FILE:?set JWT_FILE}")"
IF=$(ip route get ${PROVIDER_HOST:?} | grep -oP 'dev \K\S+')
CDNIP=${CDN_IP:?resolve the cdn test host once and export CDN_IP}
if [ "$T" = synth ]; then U="http://198.18.0.1/download/10737418240"; RS=""; else U="http://cachefly.cachefly.net/200mb.test"; RS="--resolve cachefly.cachefly.net:80:$CDNIP"; fi
restore() { [ "$T" = cdn ] && ip route replace $CDNIP/32 dev wg0 2>/dev/null; }
trap restore EXIT
pkill -x urtun-td 2>/dev/null; pkill -x urtun-2c80d68b 2>/dev/null; pkill -x ursocks-td 2>/dev/null; pkill -x ursocks-linux-p 2>/dev/null; pkill -x ursocks-2c80d68 2>/dev/null; sleep 1
env URNETWORK_NO_P2P=1 URNETWORK_PPROF_ADDR=127.0.0.1:6061 $E TUN_NAME=$DEV JWT="$JWT" API_URL="https://api.$PLATFORM_DOMAIN" PLATFORM_URL="wss://connect.$PLATFORM_DOMAIN" \
  PROVIDER_ID="$PROV" "$BIN" > /tmp/tm-$L.log 2>&1 &
CP=$!
for i in $(seq 1 40); do grep -q listening /tmp/tm-$L.log 2>/dev/null && break; sleep 1; done
grep -q listening /tmp/tm-$L.log || { echo "$L CLIENT_FAILED"; kill $CP; exit 1; }
ip addr add 10.66.0.2/24 dev $DEV 2>/dev/null; ip link set dev $DEV mtu 1100 up
ip route replace 198.18.0.0/15 dev $DEV
[ "$T" = cdn ] && ip route replace $CDNIP/32 dev $DEV
curl -s -o /dev/null $RS --max-time 6 "$U" 2>/dev/null; sleep 2
rx0=$(cat /sys/class/net/$IF/statistics/rx_bytes)
bash /root/cpuwin.sh $CP 3 $(( D - 6 )) > /tmp/tm-cpu-$L.txt &
CW=$!
T0=$(date +%s.%N); END=$(( $(date +%s) + D )); FP=()
for f in $(seq 1 $F); do
  ( : > /tmp/tm-b-$L-$f
    while [ $(date +%s) -lt $END ]; do
      curl -s -o /dev/null $RS --max-time $(( END - $(date +%s) )) -w "%{size_download}\n" "$U" >> /tmp/tm-b-$L-$f 2>/dev/null || true
    done ) & FP+=($!)
done
for p in "${FP[@]}"; do wait $p; done
T1=$(date +%s.%N); rx1=$(cat /sys/class/net/$IF/statistics/rx_bytes)
wait $CW
sleep 6
kill $CP 2>/dev/null; wait $CP 2>/dev/null
B=$(cat /tmp/tm-b-$L-* | awk '{s+=$1} END{printf "%.0f", s}')
WE=$(grep -o 'tun write errors: [0-9]*' /tmp/tm-$L.log | tail -1)
python3 -c "
el=$T1-$T0; b=$B; rx=$rx1-$rx0
print(f'bin=$(md5sum $BIN | cut -c1-12) goodput={b*8/el/1e6:.0f} wire={rx*8/el/1e6:.0f} el={el:.1f}s client[$(cat /tmp/tm-cpu-$L.txt)] ${WE:-tunwriteerr=0}')"

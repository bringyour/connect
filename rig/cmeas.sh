#!/usr/bin/env bash
# cmeas.sh LABEL FLOWS DUR TARGET(synth|cdn) [CLIENT_ENV]   (runs on the client host)
L=$1; F=$2; D=$3; T=$4; E=${5:-}
BIN=${SOCKS_BIN:-/tmp/ursocks-linux-prof}; BN=$(basename $BIN | cut -c1-15)
PROV="${PROVIDER_ID:?set PROVIDER_ID}"; JWT="$(cat "${JWT_FILE:?set JWT_FILE}")"
IF=$(ip route get ${PROVIDER_HOST:?} | grep -oP 'dev \K\S+')
if [ "$T" = synth ]; then U="http://198.18.0.1/download/10737418240"; PX="--socks5"; else U="http://cachefly.cachefly.net/200mb.test"; PX="--socks5-hostname"; fi
pkill -x ursocks-linux-c 2>/dev/null; pkill -x ursocks-linux-p 2>/dev/null; pkill -x ursocks-2c80d68 2>/dev/null; pkill -x urtun-2c80d68b 2>/dev/null; pkill -x urtun-td 2>/dev/null; pkill -x ursocks-td 2>/dev/null; sleep 1
env URNETWORK_NO_P2P=1 $E JWT="$JWT" API_URL="https://api.$PLATFORM_DOMAIN" PLATFORM_URL="wss://connect.$PLATFORM_DOMAIN" \
  PROVIDER_ID="$PROV" ADDR="127.0.0.1:9999" "$BIN" > /tmp/cm-$L.log 2>&1 &
CP=$!
for i in $(seq 1 40); do grep -q listening /tmp/cm-$L.log 2>/dev/null && break; sleep 1; done
grep -q listening /tmp/cm-$L.log || { echo "$L CLIENT_FAILED"; kill $CP; exit 1; }
# warm-up: open the path, then settle
curl -s -o /dev/null $PX 127.0.0.1:9999 --max-time 6 "$U" 2>/dev/null; sleep 2
rx0=$(cat /sys/class/net/$IF/statistics/rx_bytes)
bash /root/cpuwin.sh $CP 3 $(( D - 6 )) > /tmp/cm-cpu-$L.txt &
CW=$!
T0=$(date +%s.%N); FP=()
END=$(( $(date +%s) + D ))
for f in $(seq 1 $F); do
  ( : > /tmp/cm-b-$L-$f
    while [ $(date +%s) -lt $END ]; do
      curl -s -o /dev/null $PX 127.0.0.1:9999 --max-time $(( END - $(date +%s) )) -w "%{size_download}\n" "$U" >> /tmp/cm-b-$L-$f 2>/dev/null || true
    done ) & FP+=($!)
done
for p in "${FP[@]}"; do wait $p; done
T1=$(date +%s.%N); rx1=$(cat /sys/class/net/$IF/statistics/rx_bytes)
wait $CW
sleep 6   # let provider flows see the close while the client is still alive
kill $CP 2>/dev/null; wait $CP 2>/dev/null
B=$(cat /tmp/cm-b-$L-* | awk '{s+=$1} END{printf "%.0f", s}')
python3 -c "
el=$T1-$T0; b=$B; rx=$rx1-$rx0
print(f'bin=$(md5sum $BIN | cut -c1-12) goodput={b*8/el/1e6:.0f} wire={rx*8/el/1e6:.0f} el={el:.1f}s client[$(cat /tmp/cm-cpu-$L.txt)]')"

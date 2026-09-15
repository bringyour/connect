#!/usr/bin/env bash
# umeas.sh LABEL DUR SINK_HOST : client-side upload through the tunnel to the sink (runs on the client host)
L=$1; D=$2; SINK=$3
BIN=${SOCKS_BIN:-/tmp/ursocks-m}
PROV="${PROVIDER_ID:?set PROVIDER_ID}"; JWT="$(cat "${JWT_FILE:?set JWT_FILE}")"
for n in ursocks-m ursocks-td ursocks-ap urtun-m urtun-ap; do pkill -x $n 2>/dev/null; done; sleep 1
env URNETWORK_NO_P2P=1 ${CE:-} JWT="$JWT" API_URL="https://api.$PLATFORM_DOMAIN" PLATFORM_URL="wss://connect.$PLATFORM_DOMAIN" \
  PROVIDER_ID="$PROV" ADDR="127.0.0.1:9999" "$BIN" > /tmp/um-$L.log 2>&1 &
CP=$!
for i in $(seq 1 40); do grep -q listening /tmp/um-$L.log 2>/dev/null && break; sleep 1; done
grep -q listening /tmp/um-$L.log || { echo "$L CLIENT_FAILED"; kill $CP; exit 1; }
curl -s -o /dev/null --socks5 127.0.0.1:9999 --max-time 5 http://198.18.0.1/download/1048576; sleep 1
echo "START $(date -u +%H:%M:%S)"
curl -s -o /dev/null --socks5 127.0.0.1:9999 --max-time $D -w "curl_speed_upload=%{speed_upload}\n" -T /tmp/up.bin http://$SINK:9420/ 2>/dev/null || true
echo "END $(date -u +%H:%M:%S) bin=$(md5sum $BIN | cut -c1-12)"
sleep 4; kill $CP 2>/dev/null; wait $CP 2>/dev/null

#!/usr/bin/env bash
# ttfb-client.sh LABEL : start a socks client now, report seconds until the first successful 1 MiB fetch (60 s cap)
L=$1; BIN=${SOCKS_BIN:-/tmp/ursocks-m}
PROV="${PROVIDER_ID:?set PROVIDER_ID}"; JWT="$(cat "${JWT_FILE:?set JWT_FILE}")"
T0=$(date +%s.%N)
env URNETWORK_NO_P2P=1 JWT="$JWT" API_URL="https://api.$PLATFORM_DOMAIN" PLATFORM_URL="wss://connect.$PLATFORM_DOMAIN" \
  PROVIDER_ID="$PROV" ADDR="127.0.0.1:9999" "$BIN" > /tmp/tt-$L.log 2>&1 &
CP=$!
ok=""; tries=0
while python3 -c "import sys,time; sys.exit(0 if time.time()-$T0<60 else 1)"; do
  tries=$((tries+1))
  s=$(curl -s -o /dev/null --socks5 127.0.0.1:9999 --max-time 2 -w "%{size_download}" "http://198.18.0.1/download/1048576" 2>/dev/null)
  if [ "${s:-0}" -ge 1048576 ]; then ok=$(python3 -c "import time; print(f'{time.time()-$T0:.1f}')"); break; fi
  sleep 0.3
done
kill $CP 2>/dev/null; wait $CP 2>/dev/null
echo "ttfb=${ok:-STALL60} tries=$tries bin=$(md5sum $BIN | cut -c1-12)"

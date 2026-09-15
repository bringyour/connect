#!/usr/bin/env bash
# stallwatch.sh LABEL IFACE DIR(tx|rx) PPROF_PORT SECONDS : dump goroutines once when rate falls <50 Mb/s for 3 s after exceeding 300 Mb/s
L=$1; IF=$2; D=$3; PORT=$4; N=$5
f=/sys/class/net/$IF/statistics/${D}_bytes
last=$(cat $f); hi=0; low=0; dumped=0
for i in $(seq 1 $N); do
  sleep 1; now=$(cat $f); mbps=$(( (now-last)*8/1000000 )); last=$now
  echo "$i $mbps" >> /tmp/sw-$L.rate
  [ $mbps -gt 300 ] && hi=1
  if [ $hi = 1 ] && [ $mbps -lt 50 ]; then low=$((low+1)); else low=0; fi
  if [ $low -ge 4 ] && [ $dumped = 0 ]; then
    ss -tinm > /tmp/sw-$L.ss1 2>&1; ip netns list >/dev/null 2>&1
    curl -s -m 5 "http://127.0.0.1:$PORT/debug/pprof/goroutine?debug=2" > /tmp/sw-$L.goroutines; dumped=1
    sleep 2; ss -tinm > /tmp/sw-$L.ss2 2>&1; nstat -az 2>/dev/null | grep -i 'TcpRetransSegs\|TcpExtTCPTimeouts\|TcpExtTCPZeroWindow\|TcpExtTCPLossProbes\|TcpEstabResets\|TcpOutRsts\|TcpExtTCPAbort' > /tmp/sw-$L.nstat
    echo "DUMPED at $i" >> /tmp/sw-$L.rate
  fi
done

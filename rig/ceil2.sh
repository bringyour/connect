#!/usr/bin/env bash
# ceil.sh LABEL PROVIDER_BIN FLOWS DUR TARGET [CLIENT_ENV] [PROVIDER_ENV_LINE]  (runs on the Mac)
L=$1; PB=$2; F=$3; D=$4; T=$5; CE=${6:-}; PE=${7:-}
P=root@$PROVIDER_HOST; C=${CLIENT:-root@$CLIENT_HOST}; R=root@$RELAY_HOST
S=$(cd $(dirname $0); pwd)
bash ${RIG_TMP:-$S}/cleanleaks.sh >/dev/null 2>&1
ssh -o BatchMode=yes $P "rm -f /etc/systemd/system/urprovider.service.d/exp.conf; [ -n '$PE' ] && printf '[Service]\nEnvironment=$PE\n' > /etc/systemd/system/urprovider.service.d/exp.conf; systemctl daemon-reload; systemctl stop urprovider; cp /usr/local/bin/urprovider.$PB /usr/local/bin/urprovider; systemctl start urprovider; sleep 25"
pre=$(ssh -o BatchMode=yes $P 'echo "md5=$(md5sum /usr/local/bin/urprovider|cut -c1-12) z0=$(curl -s "http://127.0.0.1:6060/debug/pprof/goroutine?debug=2" | grep -c acquirePackAdmission)"')
# provider + relay CPU over the same window (client warm-up ~8s + 3s offset)
ssh -o BatchMode=yes $P "bash /root/cpuwin.sh \$(pgrep -x urprovider) 11 $(( D - 6 ))" > $S/pcpu-$L.txt 2>&1 &
ssh -o BatchMode=yes $R "bash /root/cpuwin.sh \$(docker inspect -f '{{.State.Pid}}' server-connect-1) 11 $(( D - 6 ))" > $S/rcpu-$L.txt 2>&1 &
MS=${CLIENT_MODE:-socks}; SCRIPT=/root/cmeas.sh; [ "$MS" = tun ] && SCRIPT=/root/tmeas.sh
res=$(ssh -o BatchMode=yes $C "SOCKS_BIN=${SOCKS_BIN:-/tmp/ursocks-linux-prof} TUN_BIN=${TUN_BIN:-/tmp/urtun-td} bash $SCRIPT $L $F $D $T '$CE'" 2>&1 | tail -1)
wait
post=$(ssh -o BatchMode=yes $P 'sleep 4; echo "z1=$(curl -s "http://127.0.0.1:6060/debug/pprof/goroutine?debug=2" | grep -c acquirePackAdmission)"')
echo "$L $PB f=$F $T client=${C#root@}/${MS} | $res provider[$(cat $S/pcpu-$L.txt)] relay[$(cat $S/rcpu-$L.txt)] | $pre $post"

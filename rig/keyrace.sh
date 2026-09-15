#!/usr/bin/env bash
# T4 dose-response: client starts 0 s vs 60 s after a provider restart; count contract verification exits.
S=$(cd $(dirname $0); pwd); P=root@$PROVIDER_HOST; C=root@$CLIENT_HOST
ssh -o BatchMode=yes $P "rm -f /etc/systemd/system/urprovider.service.d/exp.conf; systemctl daemon-reload; cp /usr/local/bin/urprovider.b-ctl /usr/local/bin/urprovider"
echo "KEYRACE START $(date +%T) provider $(ssh -o BatchMode=yes $P 'md5sum /usr/local/bin/urprovider | cut -c1-12')"
for r in $(seq 1 10); do
  if [ $((r % 2)) = 0 ]; then o="60 0"; else o="0 60"; fi
  for d in $o; do
    bash ${RIG_TMP:-$S}/cleanleaks.sh >/dev/null 2>&1
    n0=$(ssh -o BatchMode=yes $P 'wc -l < /var/log/urprovider.log')
    ssh -o BatchMode=yes $P 'systemctl restart urprovider'
    t_restart=$(date +%s)
    sleep $d
    res=$(ssh -o BatchMode=yes $C "SOCKS_BIN=/tmp/ursocks-m bash /root/cmeas.sh kr-d$d-r$r 1 20 synth" | tail -1)
    sleep 4
    ver=$(ssh -o BatchMode=yes $P "tail -n +$((n0+1)) /var/log/urprovider.log | grep -c 'exit contract verification failed'")
    auth=$(ssh -o BatchMode=yes $P "tail -n +$((n0+1)) /var/log/urprovider.log | grep -c 'auth error'")
    echo "kr d=$d r=$r | $(echo "$res" | grep -o 'bin=[0-9a-f]* goodput=[0-9]*') | contract_verification_exits=$ver auth_errors=$auth | client_start_after=${d}s"
  done
done
echo "KEYRACE DONE $(date +%T)"

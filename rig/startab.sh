#!/usr/bin/env bash
# startab.sh N : provider restart then client immediately; arms alternate: A = b-keys (15 s standby), B = b-keys-sb0 (standby at once)
S=$(cd $(dirname $0); pwd); P=root@$PROVIDER_HOST; C=root@$CLIENT_HOST; N=${1:-10}
echo "STARTAB START $(date -u +%T)"
for r in $(seq 1 $N); do
  if [ $((r % 2)) = 1 ]; then arms="A B"; else arms="B A"; fi
  for a in $arms; do
    if [ $a = A ]; then bin=urprovider.b-keys; dropin=""; else bin=urprovider.b-keys-sb; dropin="URNETWORK_STANDBY_DELAY_MS=0"; fi
    bash ${RIG_TMP:-$S}/cleanleaks.sh >/dev/null 2>&1
    ssh -o BatchMode=yes $P "systemctl stop urprovider; cp /usr/local/bin/$bin /usr/local/bin/urprovider; mkdir -p /etc/systemd/system/urprovider.service.d; if [ -n '$dropin' ]; then printf '[Service]\nEnvironment=$dropin\n' > /etc/systemd/system/urprovider.service.d/exp.conf; else rm -f /etc/systemd/system/urprovider.service.d/exp.conf; fi; systemctl daemon-reload; systemctl start urprovider; sleep 40"
    n0=$(ssh -o BatchMode=yes $P 'wc -l < /var/log/urprovider.log')
    ssh -o BatchMode=yes $P "systemctl restart urprovider"
    res=$(ssh -o BatchMode=yes $C "bash /root/ttfb-client.sh ab-$a-r$r" | tail -1)
    pm=$(ssh -o BatchMode=yes $P "md5sum /usr/local/bin/urprovider | cut -c1-12; tail -n +$((n0+1)) /var/log/urprovider.log | grep -c 'standby transport dials'; tail -n +$((n0+1)) /var/log/urprovider.log | grep -c 'exit contract verification failed'" | tr '\n' ' ')
    echo "ab arm=$a r=$r | $res | provider md5/standbyDialLogs/verifyExits: $pm"
  done
done
ssh -o BatchMode=yes $P "rm -f /etc/systemd/system/urprovider.service.d/exp.conf; systemctl daemon-reload"
echo "STARTAB DONE $(date -u +%T)"

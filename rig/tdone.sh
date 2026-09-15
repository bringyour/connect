#!/usr/bin/env bash
# tdone.sh LABEL FLOWS TARGET "PROVIDER_ENV_EXTRA" : one tdiag run + parsed gate line
L=$1; F=$2; T=$3; EX=${4:-}; S=$(cd $(dirname $0); pwd); P=root@$PROVIDER_HOST
PE="URNETWORK_TDIAG=1"; [ -n "$EX" ] && PE="$PE $EX"
n0=$(ssh -o BatchMode=yes $P 'wc -l < /var/log/urprovider.log')
out=$(bash $S/ceil2.sh $L ${PB:-tdiag} $F ${DUR:-40} $T "${CE:-}" "$PE")
ssh -o BatchMode=yes $P "tail -n +$((n0+1)) /var/log/urprovider.log | grep TDIAG" > $S/td-$L.txt
g=$(python3 $S/tdparse.py $S/td-$L.txt | tail -1 | cut -d' ' -f3-)
echo "$out [$EX] || $g"

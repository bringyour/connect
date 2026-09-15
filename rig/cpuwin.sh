#!/usr/bin/env bash
# cpuwin.sh PID DELAY WINDOW -> "cores=<process cores> hot=<hottest thread %> threads>50%=<n>"
PID=$1; DELAY=$2; WIN=$3; HZ=$(getconf CLK_TCK)
sleep "$DELAY"
snap() { for t in /proc/$PID/task/*; do awk -v t="${t##*/}" '{print t, $14+$15}' "$t/stat" 2>/dev/null; done; }
a=$(snap); pa=$(awk '{print $14+$15}' /proc/$PID/stat 2>/dev/null)
sleep "$WIN"
b=$(snap); pb=$(awk '{print $14+$15}' /proc/$PID/stat 2>/dev/null)
join <(echo "$a" | sort) <(echo "$b" | sort) | awk -v hz="$HZ" -v w="$WIN" -v pa="$pa" -v pb="$pb" '
  { d=($3-$2)/hz/w*100; if (d>hot) hot=d; if (d>50) n++ }
  END { printf "cores=%.2f hot=%.0f%% threads>50%%=%d", (pb-pa)/hz/w, hot, n+0 }'

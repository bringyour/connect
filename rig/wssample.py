# wssample.py LABEL SECONDS : each second, TCP_INFO of the busiest established connection to the relay (dst :443)
import subprocess, re, sys, time, json, os
label, n = sys.argv[1], int(sys.argv[2])
out = open(f"/tmp/ws-{label}.jsonl", "w")
keys = {"cwnd": r"cwnd:(\d+)", "rtt": r"\brtt:([\d.]+)/", "rttvar": r"\brtt:[\d.]+/([\d.]+)", "bytes_received": r"bytes_received:(\d+)",
        "retrans_total": r"retrans:\d+/(\d+)", "lost": r"\blost:(\d+)", "rcv_space": r"rcv_space:(\d+)", "rcv_ssthresh": r"rcv_ssthresh:(\d+)",
        "rb": r"skmem:\(r\d+,rb(\d+)", "r": r"skmem:\(r(\d+)", "d": r"skmem:\([^)]*,d(\d+)\)", "rcv_rtt": r"rcv_rtt:([\d.]+)",
        "reord": r"reord_seen:(\d+)", "rcv_ooopack": r"rcv_ooopack:(\d+)", "snd_wnd": r"snd_wnd:(\d+)", "delivery_rate": r"delivery_rate (\d+)bps"}
for i in range(n):
    time.sleep(1)
    txt = subprocess.run(["ss", "-tinm", "state", "established", "dst", os.environ["RELAY_HOST"]], capture_output=True, text=True).stdout
    blocks = re.split(r"\n(?=\S)", txt)
    best = None
    for b in blocks:
        if ":443" not in b: continue
        m = re.search(r"bytes_received:(\d+)", b)
        if m and (best is None or int(m.group(1)) > best[0]): best = (int(m.group(1)), b)
    if best:
        rec = {"t": i}
        for k, p in keys.items():
            m = re.search(p, best[1]); rec[k] = float(m.group(1)) if m else None
        out.write(json.dumps(rec) + "\n"); out.flush()

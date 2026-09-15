import socket, threading, time, sys
port = int(sys.argv[1])
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("0.0.0.0", port)); srv.listen(64)
total = 0; lock = threading.Lock()
def handle(c):
    global total
    with c:
        while True:
            b = c.recv(1 << 20)
            if not b: break
            with lock: total += len(b)
def rate():
    last = 0
    while True:
        time.sleep(1)
        with lock: t = total
        print(f"{time.strftime('%H:%M:%S')} total={t} rate_mbps={(t-last)*8/1e6:.1f}", flush=True); last = t
threading.Thread(target=rate, daemon=True).start()
while True:
    c, _ = srv.accept(); threading.Thread(target=handle, args=(c,), daemon=True).start()

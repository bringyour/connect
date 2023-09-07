sudo ip tuntap add mode tun tun0

sudo ip link set dev tun0 up

sudo ip address add 10.0.0.1 dev tun0

# (brien) brien ~/bringyour/connect/test-ip-local $ ls -lah /sys/class/net/tun0
# lrwxrwxrwx 1 root root 0 Aug 29 16:22 /sys/class/net/tun0 -> ../../devices/virtual/net/tun0



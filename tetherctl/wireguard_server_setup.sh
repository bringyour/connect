# enable ipv4 forwarding
sysctl -w net.ipv4.ip_forward=1
# TODO: make it permanent

export WG_ADDRESS=192.168.88.1/24

# delete old interface
if ip -br link | grep wg0 ; then
   echo "deleting old wg0 interface"
   ip link delete wg0
fi

ip link add dev wg0 type wireguard
ip address add dev wg0 $WG_ADDRESS
# for this to run need this to be available which it is not if running with sudo
go run main.go setup-device --device_name=wg0 --listen_port=51820

# TODO: make persistent config

# TODO: CHECK IF THIS IS THE CASE:
# every time a peer is added to config, we need to add it with ip route and to persistent config?????
# i.e., everytime peer add itself need to do ip route add <peer_allowed_ip> dev wg0
# e.g., ip route add 192.168.88.101/32 dev wg0

ip link set up dev wg0
export OUR_INTERFACE=eth0
iptables -A FORWARD -i %i -j ACCEPT; iptables -A FORWARD -o %i -j ACCEPT; iptables -t nat -A POSTROUTING -o $OUR_INTERFACE -j MASQUERADE

# check the created interface
ip addr show wg0

# check rules using:
# sudo iptables -L FORWARD -v -n
# sudo iptables -t nat -L POSTROUTING -v -n

# when removing use:
# ip link delete dev wg0
# iptables -D FORWARD -i %i -j ACCEPT; iptables -D FORWARD -o %i -j ACCEPT; iptables -t nat -D POSTROUTING -o $OUR_INTERFACE -j MASQUERADE
# TODO: automate finding interface name for routing

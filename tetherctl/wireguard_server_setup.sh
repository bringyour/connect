# uncomment next line if you want to try with kernel module 
# ip link add dev wg0 type wireguard 
# if not then install wireguard-go and create wg0 interface manually before running script (to disable kernel module check bottom of this script)
# https://github.com/WireGuard/wireguard-go


# enable ipv4 forwarding
sysctl -w net.ipv4.ip_forward=1
# TODO: make it permanent

export WG_ADDRESS=192.168.88.1/24
ip address add dev wg0 $WG_ADDRESS
go run main.go setup-device --device_name=wg0 --listen_port=51820
ip link set up dev wg0

# iptables rules (not always neccessary)
export OUR_INTERFACE=eth0
iptables -A FORWARD -i wg0 -j ACCEPT; iptables -A FORWARD -o wg0 -j ACCEPT; iptables -t nat -A POSTROUTING -o $OUR_INTERFACE -j MASQUERADE

# check the created interface
ip addr show wg0

# check rules using:
# sudo iptables -L FORWARD -v -n
# sudo iptables -t nat -L POSTROUTING -v -n
# for chekcing if packets are matching on rules:
# sudo watch -n 5 "iptables -nvL FORWARD; iptables -t nat -nvL POSTROUTING | grep MASQUERADE"

# when removing use:
# ip link delete dev wg0
# iptables -D FORWARD -i wg0 -j ACCEPT; iptables -D FORWARD -o wg0 -j ACCEPT; iptables -t nat -D POSTROUTING -o eth0 -j MASQUERADE
# TODO: automate finding interface name for routing

# unload wireguard kernel module $ sudo rmmod wireguard
# check if it is unloaded $ lsmod | grep wireguard
# balcklist module from laoding automatically $ sudo nano /etc/modprobe.d/blacklist-wireguard.conf
# put in file $ blacklist wireguard
# reboot system

# to undo
# $ sudo rm /etc/modprobe.d/blacklist-wireguard.conf
# $ sudo modprobe wireguard
# reboot system


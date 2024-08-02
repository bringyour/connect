package tether

import (
	"fmt"
	"net"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

type IPVersion int

const (
	IPv4   IPVersion = 4
	IPv6   IPVersion = 6
	AllIPs IPVersion = 0 // represents both IPv4 and IPv6
)

// getNextAllowedIP finds the next available IP in any of the addresses of the server's interface.
//
// Note: if concurrency is introduced at some point this might have conflicts
func (server *TetherClient) getNextAllowedIP(ipVersion IPVersion) (string, error) {
	ipv4s, err := getInterfaceAddresses(server.DeviceName, ipVersion)
	if err != nil {
		return "", err
	}

	if len(ipv4s) == 0 {
		return "", fmt.Errorf("no IPv4 addresses found for interface %s", server.DeviceName)
	}

	device, err := server.Device(server.DeviceName)
	if err != nil {
		return "", fmt.Errorf("device %q: %w", server.DeviceName, err)
	}

	for _, subnet := range ipv4s {
		availableIP, err := getSubnetAvailableIP(subnet, device.Peers)
		if err == nil {
			return availableIP.String(), nil
		}
	}

	return "", fmt.Errorf("no available IPs in any of the addresses of the device")
}

// function getInterfaceAddresses returns the IP addresses of the given interface on the host machine.
//
// interfaceName is the name of the interface for which to get the IP addresses.
// ipVersion specifies which subset of addresses to return (IPv4, IPv6 or AllIPs).
//
// Returns an error if the interface does not exist or if there is an error getting the addresses.
func getInterfaceAddresses(interfaceName string, ipVersion IPVersion) ([]net.IPNet, error) {
	intf, err := net.InterfaceByName(interfaceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get interface %s: %v", interfaceName, err)
	}

	addrs, err := intf.Addrs()
	if err != nil {
		return nil, fmt.Errorf("failed to get addresses for interface %s: %v", interfaceName, err)
	}

	var ipnets []net.IPNet
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok {
			continue // skip non-IPNet addresses
		}

		// filter based on IP version or include all if ipVersion is AllIPs
		if ipVersion == AllIPs || (ipVersion == IPv4 && isIPv4(ipnet.IP)) || (ipVersion == IPv6 && isIPv6(ipnet.IP)) {
			ipnets = append(ipnets, *ipnet)
		}
	}

	return ipnets, nil
}

// getSubnetAvailableIP finds the next available IP in the given subnet that is not already used by any of the peers and is not the network or broadcast address.
//
// subnet is the subnet for which to find the available IP.
// peers is the list of peers that are already connected to the device.
func getSubnetAvailableIP(subnet net.IPNet, peers []wgtypes.Peer) (net.IP, error) {
	// mark IPs that are already used
	usedIPs := make(map[string]bool)
	for _, peer := range peers {
		for _, allowedIP := range peer.AllowedIPs {
			if subnet.Contains(allowedIP.IP) {
				usedIPs[allowedIP.IP.String()] = true
			}
		}
	}

	// calculate the network and broadcast addresses
	networkIP := subnet.IP.Mask(subnet.Mask)
	broadcastIP := calculateBroadcastAddr(subnet)

	// find availabe IP (that is not the network or broadcast IP)
	for ip := networkIP.Mask(subnet.Mask); subnet.Contains(ip); incrementIP(ip) {
		if !ip.Equal(networkIP) && !ip.Equal(broadcastIP) && !usedIPs[ip.String()] {
			return ip, nil
		}
	}

	return nil, fmt.Errorf("no available IPs in the subnet")
}

// calculateBroadcastAddr calculates the broadcast address for a given subnet.
//
// It is the subnet mask with 1s for the host part of the IP address,
// i.e., for 10.10.10.10/24 it is 10.10.10.255 or for 10.10.10.10/30 it is 10.10.10.13.
func calculateBroadcastAddr(subnet net.IPNet) net.IP {
	ip := subnet.IP.To4()
	if ip == nil {
		ip = subnet.IP.To16()
	}

	broadcast := make(net.IP, len(ip))
	copy(broadcast, ip)

	for i := range broadcast {
		broadcast[i] |= ^subnet.Mask[i]
	}

	return broadcast
}

// incrementIP increments an IP address by 1.
//
// because of IPv4-mapped IPv6 it is possible to increment from an IPv6 to an IPv4 (::fffe:ffff:ffff -> 0.0.0.0)
func incrementIP(ip net.IP) {
	if isIPv4(ip) {
		ip = ip.To4() // make sure IPv4 addresses do not overflow into IPv6 space
	}
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
}

// isIPv4 checks if the given IP address is an IPv4 address.
//
// Examples of valid IPv4 IPs: "192.168.1.1", "0.0.0.0", "::ffff:192.0.2.1" (IPv4-mapped IPv6 address)
func isIPv4(ip net.IP) bool {
	return ip.To4() != nil
}

// isIPv6 checks if the given IP address is an IPv6 address.
//
// Examples of valid IPv6 IPs: "::1", "2001:db8::68", "::", "0:0:0:0:0:0:c0a8:5901" (ipv6 with leading zeros).
// This one is invalid "::ffff:256.0.2.1" because it goes into the reserved space from IPv4-mapped IPv6.
func isIPv6(ip net.IP) bool {
	return ip.To16() != nil && ip.To4() == nil
}

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

func (ipv IPVersion) String() string {
	switch ipv {
	case IPv4:
		return "IPv4"
	case IPv6:
		return "IPv6"
	case AllIPs:
		return "IPv4 and IPv6"
	default:
		return "Unknown"
	}
}

// getNextAllowedIP finds the next available IP in any of the addresses of a device on the Client.
//
// Returns an error if the device has no addresses of the specified ipVersion which can be checked using errors.Is(err, ErrorAIPsNoAddressesFound).
// Returns an error if no available IPs are found which can be checked using errors.Is(err, ErrorAIPsNoAvailableIP).
// If the device by the same name does not exist an error is returned which can be checked using errors.Is(err, ErrDeviceNotFound).
//
// Note: if concurrency is introduced at some point this function will have conflicts
func (c *Client) getNextAllowedIP(deviceName string, ipVersion IPVersion) (string, error) {
	addrs, err := c.GetAddressesFromDevice(deviceName)
	if err != nil {
		return "", err
	}

	subnets := filterAddresses(addrs, ipVersion)
	if len(subnets) == 0 {
		return "", fmt.Errorf("%w for device %s of type %s", ErrorAIPsNoAddressesFound, deviceName, ipVersion.String())
	}

	device, err := c.Device(deviceName)
	if err != nil {
		return "", err
	}

	for _, subnet := range subnets {
		availableIP, err := getSubnetAvailableIP(subnet, device.Peers)
		if err == nil {
			return availableIP.String(), nil
		}
	}

	return "", ErrorAIPsNoAvailableIP
}

// filterAddresses filters the addresses based on the IP version.
// If ipVersion is AllIPs all addresses are returned.
//
// Non-IPNet addresses are skipped.
// Addresses are returned as the network addresses of the network, i.e., 1.2.3.4/24 will become 1.2.3.0/24.
func filterAddresses(addrs []string, ipVersion IPVersion) []net.IPNet {
	var ipnets []net.IPNet
	for _, addr := range addrs {
		_, ipnet, err := net.ParseCIDR(addr)
		if err != nil {
			continue // skip non-IPNet addresses
		}

		// filter based on IP version or include all if ipVersion is AllIPs
		if ipVersion == AllIPs || (ipVersion == IPv4 && isIPv4(ipnet.IP)) || (ipVersion == IPv6 && isIPv6(ipnet.IP)) {
			ipnets = append(ipnets, *ipnet)
		}
	}

	return ipnets
}

// getSubnetAvailableIP finds the next available IP in the given subnet that is not already used by any of the peers and is not the network or broadcast address.
//
// subnet is the subnet for which to find the available IP.
// peers is the list of peers that are already connected to the device.
//
// If no available IP is found, an error is returned which can be checked using errors.Is(err, ErrorAIPsNoAvailableIP).
func getSubnetAvailableIP(subnet net.IPNet, peers []wgtypes.Peer) (net.IPNet, error) {
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
			// make mask full so that the ipnet is just one ip
			return net.IPNet{IP: ip, Mask: net.CIDRMask(8*len(ip), 8*len(ip))}, nil
		}
	}

	return net.IPNet{}, ErrorAIPsNoAvailableIP
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

package tether

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// TetherClient is a wrapper around the wgctrl.Client that provides additional functionality.
// It contains the client's IP and the device name.
type TetherClient struct {
	*wgctrl.Client
	ClientIP   net.IP
	DeviceName string
}

var lastAllowedIP string = "192.168.88.100/32" // TODO: figure out how IPs are chosen. for now its always the same

// SetupDevice sets up a new (or changes an old) WireGuard device based on the DeviceName in the TetherClient.
// The listenPort and privateKey can be configured where if privateKey is nil then a random one is generated.
// The function returns an error if the device could not be configured.
func (server *TetherClient) SetupDevice(listenPort int, privateKey *wgtypes.Key) error {
	if privateKey == nil {
		// generate new private key if none is provided
		privateKeyGenerated, err := wgtypes.GeneratePrivateKey()
		if err != nil {
			return fmt.Errorf("failed to generate private key: %v", err)
		}
		privateKey = &privateKeyGenerated // publicKey := privateKey.PublicKey()
	}

	// configure the device with the new (or given) private key
	err := server.ConfigureDevice(server.DeviceName, wgtypes.Config{
		PrivateKey: privateKey,
		ListenPort: &listenPort,
	})
	return err
}

// GetAvailableDevices returns a list of all available devices on the server.
func (server *TetherClient) GetAvailableDevices() ([]string, error) {
	devices, err := server.Devices()
	if err != nil {
		return nil, err
	}

	deviceNames := make([]string, 0, len(devices))
	for _, device := range devices {
		deviceNames = append(deviceNames, device.Name)
	}

	return deviceNames, nil
}

// GetDeviceInterface returns a string representation of the device based on the DeviceName in the TetherClient.
func (server *TetherClient) GetDeviceInterface() (string, error) {
	device, err := server.Device(server.DeviceName)
	if err != nil {
		return "", fmt.Errorf("device %q does not exist", server.DeviceName)
	}

	return fmt.Sprintf(`Device %q 
  Type = %s
  PrivateKey = %s
  PublicKey = %s
  ListenPort = %d
  FirewallMark = %d
  Peers = %+v
`, device.Name, device.Type, device.PrivateKey, device.PublicKey, device.ListenPort, device.FirewallMark, device.Peers), nil
}

// AddPeerToDevice adds a new peer to the device based on the public key provided.
// The function returns the config for the new peer if successful.
func (server *TetherClient) AddPeerToDevice(pubKey wgtypes.Key) (string, error) {
	peerIP, err := getNextAllowedIP(lastAllowedIP)
	if err != nil {
		return "", fmt.Errorf("failed to get next allowed IP (%v)", err)
	}

	_, peerIPNet, err := net.ParseCIDR(peerIP)
	if err != nil {
		return "", fmt.Errorf("failed to parse peer IP: %q", peerIP)
	}

	allowedIPs := []net.IPNet{*peerIPNet}

	newPeer := wgtypes.PeerConfig{
		PublicKey:  pubKey,
		AllowedIPs: allowedIPs,
	}

	err = server.ConfigureDevice(server.DeviceName, wgtypes.Config{
		Peers: []wgtypes.PeerConfig{newPeer},
	})
	if err != nil {
		return "", err
	}

	// we have added the peer now we need to make the config
	return server.createConfigForPeer(peerIP, pubKey.String())
}

// GetPeerConfig returns the config for a peer based on the public key provided.
func (server *TetherClient) GetPeerConfig(peerPubKey string) (string, error) {
	device, err := server.Device(server.DeviceName)
	if err != nil {
		return "", fmt.Errorf("device %q does not exist", server.DeviceName)
	}

	_pk, err := wgtypes.ParseKey(peerPubKey)
	if err != nil {
		return "", err
	}

	// get peer with corresponding public key and return config
	for _, peer := range device.Peers {
		if peer.PublicKey == _pk {
			return server.createConfigForPeer(ipsString(peer.AllowedIPs), peerPubKey)
		}
	}

	// peer not found
	return "", fmt.Errorf("peer with public key %q not found", peerPubKey)
}

// createConfigForPeer creates a new config for a peer based on the provided allowed IP and public key.
func (server *TetherClient) createConfigForPeer(peerAllowedIP string, pubKey string) (string, error) {
	device, err := server.Device(server.DeviceName)
	if err != nil {
		return "", fmt.Errorf("device %q does not exist", server.DeviceName)
	}

	newPeer := fmt.Sprintf(`# Config for public key %q
[Interface]
PrivateKey = __PLACEHOLDER__ # replace __PLACEHOLDER__ with your private key
Address = %s
DNS = 1.1.1.1
	
[Peer]
PublicKey = %s
AllowedIPs = 0.0.0.0/0
Endpoint = %s:%d
	`,
		pubKey,
		peerAllowedIP,
		device.PublicKey.String(),
		server.ClientIP.String(),
		device.ListenPort)

	return newPeer, nil
}

// crude implementation of getting the next allowed IP
func getNextAllowedIP(lastAllowedIp string) (string, error) {
	ipString, ipNet, err := net.ParseCIDR(lastAllowedIp)
	if err != nil {
		return "", err
	}

	ones, _ := ipNet.Mask.Size()

	ip := ipString.To4()
	if ip == nil {
		return "", fmt.Errorf("not an IPv4 address")
	}

	lastOctetString := strings.Split(ipString.String(), ".")[3] // get last octet

	// convert the last octet string to an integer
	lastOctet, err := strconv.Atoi(lastOctetString)
	if err != nil {
		return "", fmt.Errorf("invalid last octet")
	}

	lastOctet++
	// for now if last octet is over 255 we return an error
	if lastOctet > 255 {
		return "", fmt.Errorf("IP overflow")
	}

	ip[3] = byte(lastOctet)

	// Convert the updated IP back to a string
	newIP := fmt.Sprintf("%d.%d.%d.%d/%d", ip[0], ip[1], ip[2], ip[3], ones)

	return newIP, nil
}

func ipsString(ipns []net.IPNet) string {
	ss := make([]string, 0, len(ipns))
	for _, ipn := range ipns {
		ss = append(ss, ipn.String())
	}

	return strings.Join(ss, ", ")
}

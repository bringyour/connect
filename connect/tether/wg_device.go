package tether

import (
	"fmt"
	"net"
	"strings"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// Device is an abstarction from a WireGuard interface.
// Having a device assumes that the corresponding interface exists,
// i.e., you cannot create a Device it "becomes" one after creating a WireGuard interface
// and configuring it with the desired settings (see wg_config.go for config options)

// ChangeDevice edits the config of an existing WireGuard device based on the DeviceName in the Client.
//
// listenPort specifies the port to which the device should listen for peers (if nil it is not applied).
// privateKey specifies the private key of the device (if nil it is not applied).
//
// The function returns an error if the device could not be configured.
func (c *Client) ChangeDevice(privateKey *wgtypes.Key, listenPort *int) error {
	if privateKey == nil && listenPort == nil {
		fmt.Printf("Warning! No changes requested to device %s.\n", c.DeviceName)
		return nil

	}

	//  at least one of the fields is not nil
	return c.ConfigureDevice(c.DeviceName, wgtypes.Config{
		PrivateKey: privateKey, // if nil it is not applied
		ListenPort: listenPort, // if nil it is not applied
	})
}

// GetAvailableDevices returns a list of all available devices on the client.
func (c *Client) GetAvailableDevices() []string {
	devices := c.Devices()
	deviceNames := make([]string, 0, len(devices))
	for name := range devices {
		deviceNames = append(deviceNames, name)
	}
	return deviceNames
}

// GetDeviceInterface returns a string representation of the device based on the DeviceName in the Client.
//
// The function returns an error if the device could not be retrieved.
func (c *Client) GetDeviceFormatted() (string, error) {
	device, err := c.Device(c.DeviceName)
	if err != nil {
		return "", fmt.Errorf("device %q does not exist", c.DeviceName)
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
// If the peer already exists it just adds a new IP to its AllowedIPs.
//
// The function returns an error if a next AllowedIP could not be generated or if the peer could not be added.
func (c *Client) AddPeerToDevice(pubKey wgtypes.Key) error {
	peerIP, err := c.getNextAllowedIP(IPv4)
	if err != nil {
		return fmt.Errorf("next allowed IP: %w", err)
	}
	peerIP += "/32" // TODO: when using IPv6 this should be /124 or maybe just getNextAllowedIP should do it

	_, peerIPNet, err := net.ParseCIDR(peerIP)
	if err != nil {
		return fmt.Errorf("parsing peer IP %q: %w", peerIP, err)
	}

	allowedIPs := []net.IPNet{*peerIPNet}

	newPeer := wgtypes.PeerConfig{
		PublicKey:  pubKey,
		AllowedIPs: allowedIPs,
	}

	return c.ConfigureDevice(c.DeviceName, wgtypes.Config{
		Peers: []wgtypes.PeerConfig{newPeer},
	})
}

// GetPeerConfig returns the formatted config for a peer based on the public key provided and the endpoint of the server.
func (c *Client) GetPeerConfig(peerPubKey string, endpoint string) (string, error) {
	device, err := c.Device(c.DeviceName)
	if err != nil {
		return "", fmt.Errorf("device %q does not exist", c.DeviceName)
	}

	_pk, err := wgtypes.ParseKey(peerPubKey)
	if err != nil {
		return "", err
	}

	// get peer with corresponding public key and return config
	for _, peer := range device.Peers {
		if peer.PublicKey == _pk {
			return c.createConfigForPeer(ipsString(peer.AllowedIPs), peerPubKey, endpoint)
		}
	}

	// peer not found
	return "", fmt.Errorf("peer with public key %q not found", peerPubKey)
}

// createConfigForPeer creates a new config (ini format) for a peer based on the provided allowed IP and public key.
//
// The PrivateKey field is set to a __PLACEHOLDER__ which should be replaced by the peer to make a valid config.
// No other __PLACEHOLDER__ exists in the config.
func (c *Client) createConfigForPeer(peerAllowedIP string, pubKey string, endpoint string) (string, error) {
	device, err := c.Device(c.DeviceName)
	if err != nil {
		return "", fmt.Errorf("device %q does not exist", c.DeviceName)
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
		endpoint,
		device.ListenPort)

	return newPeer, nil
}

func ipsString(ipnets []net.IPNet) string {
	ipNetStrings := make([]string, 0, len(ipnets))
	for _, ipnet := range ipnets {
		ipNetStrings = append(ipNetStrings, ipnet.String())
	}
	return strings.Join(ipNetStrings, ", ")
}

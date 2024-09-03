package tether

import (
	"fmt"
	"net"
	"os/exec"
	"strings"

	"bringyour.com/wireguard/tun"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

const TETHER_CMD string = "[sh]"

// BringUpDevice reads the configuration file and attempts to bring up the WireGuard device from the Client's devices.
//
// The function overrides the peers and addresses of the device with the ones in the provided configuration file.
//
// The function returns an error if the device could not be retrieved, the private key could not be parsed or if the configuration could not be applied.
// Additionally, an error will be returned if the ByWgConfig.Name does not match the deviceName which can be checked using errors.Is(err, ErrNameMismatch).
func (c *Client) BringUpDevice(deviceName string, bywgConf ByWgConfig) error {
	if deviceName != bywgConf.Name {
		return ErrNameMismatch
	}
	if _, err := c.Device(deviceName); err != nil {
		return err
	}

	runCommands(bywgConf.PreUp, deviceName) // pre up commnds

	// setup device

	privateKey, err := wgtypes.ParseKey(bywgConf.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %w", err)
	}

	err = c.ConfigureDevice(deviceName, wgtypes.Config{
		PrivateKey:   &privateKey,
		ListenPort:   bywgConf.ListenPort, // if nil it is not applied
		ReplacePeers: true,
		Peers:        bywgConf.Peers,
	})
	if err != nil {
		return fmt.Errorf("failed to configure device: %w", err)
	}

	err = c.AddAddressesToDevice(deviceName, bywgConf.Address, true) // add addresses
	if err != nil {
		return err
	}

	// bring up device

	if err := c.AddEventToDevice(deviceName, tun.EventUp); err != nil {
		return fmt.Errorf("error running up event: %w", err)
	}

	runCommands(bywgConf.PostUp, deviceName) // post up commands

	return nil
}

// BringDownDevice brings down a WireGuard device based on a configuration file.
// Additionally, if the SaveConfig option is set in the configuration file, the updated configuration is saved to the specified location.
//
// The function returns an error if the device could not be retrieved.
// Additionally, an error will be returned if the ByWgConfig.Name does not match the deviceName which can be checked using errors.Is(err, ErrNameMismatch).
func (c *Client) BringDownDevice(deviceName string, bywgConf ByWgConfig, configSavePath string) error {
	if deviceName != bywgConf.Name {
		return ErrNameMismatch
	}
	if _, err := c.Device(deviceName); err != nil {
		return err
	}

	runCommands(bywgConf.PreDown, deviceName)
	if bywgConf.SaveConfig {
		c.SaveConfigToFile(deviceName, bywgConf, configSavePath)
	}
	if err := c.AddEventToDevice(deviceName, tun.EventDown); err != nil {
		return fmt.Errorf("error running down event: %w", err)
	}
	runCommands(bywgConf.PostDown, deviceName)

	return nil
}

// GetDeviceInterface returns a string representation of a device including its addresses.
//
// The function returns an error if the device could not be retrieved.
func (c *Client) GetDeviceFormatted(deviceName string) (string, error) {
	device, err := c.Device(deviceName)
	if err != nil {
		return "", err
	}
	addrs, err := c.GetAddressesFromDevice(deviceName)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`Device %q 
  Type = %s
  PrivateKey = %s
  PublicKey = %s
  ListenPort = %d
  FirewallMark = %d
  Peers = %+v
  Addresses = %s
`, device.Name, device.Type, device.PrivateKey, device.PublicKey, device.ListenPort, device.FirewallMark, device.Peers, strings.Join(addrs, ", ")), nil
}

// AddPeerToDevice adds a new peer to the device based on the public key provided.
//
// The function returns an error if the device could not be retrieved, a peer with the same public key already exists,
// a next AllowedIP could not be generated or if the peer could not be added.
func (c *Client) AddPeerToDevice(deviceName string, pubKey wgtypes.Key) error {
	device, err := c.Device(deviceName)
	if err != nil {
		return err
	}
	for _, peer := range device.Peers {
		if peer.PublicKey == pubKey {
			return fmt.Errorf("peer with public key %q already exists", pubKey)
		}
	}

	peerIP, err := c.getNextAllowedIP(deviceName, IPv4)
	if err != nil {
		return fmt.Errorf("next allowed IP: %w", err)
	}

	_, peerIPNet, err := net.ParseCIDR(peerIP)
	if err != nil {
		return fmt.Errorf("parsing peer IP %q: %w", peerIP, err)
	}

	allowedIPs := []net.IPNet{*peerIPNet}

	newPeer := wgtypes.PeerConfig{
		PublicKey:  pubKey,
		AllowedIPs: allowedIPs,
	}

	return c.ConfigureDevice(deviceName, wgtypes.Config{
		Peers: []wgtypes.PeerConfig{newPeer},
	})
}

// RemovePeerFromDevice removes a peer from a device based on the public key provided.
// If the peer is not found, nothing happens.
//
// The function returns an error if the peer could not be removed.
func (c *Client) RemovePeerFromDevice(deviceName string, pubKey wgtypes.Key) error {
	newPeer := wgtypes.PeerConfig{
		Remove:     true,
		UpdateOnly: true,
		PublicKey:  pubKey,
	}

	return c.ConfigureDevice(deviceName, wgtypes.Config{
		Peers: []wgtypes.PeerConfig{newPeer},
	})
}

// GetPeerConfig returns the (ini) formatted config for a peer based on the public key provided and the endpoint of the server.
//
// An error is returned if the device could not be retrieved, the public key is invalid or the peer is not found.
func (c *Client) GetPeerConfig(deviceName string, peerPubKey string, endpoint string) (string, error) {
	device, err := c.Device(deviceName)
	if err != nil {
		return "", err
	}

	_pk, err := wgtypes.ParseKey(peerPubKey)
	if err != nil {
		return "", err
	}

	// get peer with corresponding public key and return config
	for _, peer := range device.Peers {
		if peer.PublicKey == _pk {
			return c.createConfigForPeer(deviceName, ipsString(peer.AllowedIPs), peerPubKey, endpoint)
		}
	}

	// peer not found
	return "", fmt.Errorf("peer with public key %q not found", peerPubKey)
}

// createConfigForPeer creates a new config (ini format) for a peer based on the provided allowed IP, public key and endpoint.
//
// An error is returned if the device could not be retrieved.
//
// The PrivateKey field is set to a __PLACEHOLDER__ which should be replaced by the peer to make a valid config.
// No other __PLACEHOLDER__ exists in the config.
func (c *Client) createConfigForPeer(deviceName, peerAllowedIP string, pubKey string, endpoint string) (string, error) {
	device, err := c.Device(deviceName)
	if err != nil {
		return "", err
	}

	newPeer := fmt.Sprintf(`# Config for public key %q
[Interface]
PrivateKey = __PLACEHOLDER__ # replace with your private key
Address = %s
DNS = 1.1.1.1, 8.8.8.8
	
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

// ipsString transforms a list of ips to a string where the ips are separated by a ","
func ipsString(ipnets []net.IPNet) string {
	ipNetStrings := make([]string, 0, len(ipnets))
	for _, ipnet := range ipnets {
		ipNetStrings = append(ipNetStrings, ipnet.String())
	}
	return strings.Join(ipNetStrings, ", ")
}

func runCommands(commands []string, deviceName string) {
	for _, cmd := range commands {
		cmd = strings.Replace(cmd, "%i", deviceName, -1) // substitute %i with device name
		fmt.Printf("%s %s\n", TETHER_CMD, cmd)
		output, _ := exec.Command("sh", "-c", cmd).CombinedOutput()
		// errors are printed with normal output
		fmt.Print(string(output))
	}
}

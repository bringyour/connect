package tether

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

type ByWgConfig struct {
	Name       string // inferred from config file name
	Address    []string
	PrivateKey string // mandatory
	ListenPort *int
	PreUp      []string
	PostUp     []string
	PreDown    []string
	PostDown   []string
	SaveConfig bool
	Peers      []wgtypes.PeerConfig
}

// GetUpdatedConfig returns the updated config file as a string based on the provided config and the device,
// i.e., ListenPort, PrivateKey and Peers are updated from the device.
//
// config is the base ByWgConfig configuration of the device.
//
// The function returns an error if the device cannot be retrieved or the config name doesn't match device name.
func (server *TetherClient) GetUpdatedConfig(config ByWgConfig) (string, error) {
	if server.DeviceName != config.Name {
		return "", fmt.Errorf("name in config does not match the device name")
	}

	device, err := server.Device(server.DeviceName)
	if err != nil {
		return "", fmt.Errorf("device %q does not exist", server.DeviceName)
	}

	newConfig := updateConfigFromDevice(config, device)
	return configToString(newConfig), nil
}

// SaveConfigToFile updates the config using GetUpdatedConfig() and saves it in the specified filePath.
// The file is saved with -rw-r--r-- (0644) permissions.
//
// config is the ByWgConfig of the device.
// filePath is the place where config is saved.
//
// The function returns an error if the updated config could not be retrieved or the file was not saved properly.
func (server *TetherClient) SaveConfigToFile(config ByWgConfig, filePath string) error {
	content, err := server.GetUpdatedConfig(config)
	if err != nil {
		return fmt.Errorf("updated config file: %w", err)
	}

	return os.WriteFile(filePath, []byte(content), 0644)
}

// PerseConfig transforms a textual representation of a ByWireGuard configuration into a ByWgConfig struct.
//
// filePath is the location of the textual representatin of the config.
//
// The function returns an error if the file could not be retrieved or the configuration is not a valid configuration.
func ParseConfig(filePath string) (ByWgConfig, error) {
	configName := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))

	file, err := os.Open(filePath)
	if err != nil {
		return ByWgConfig{}, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	// TODO make sure this is a proper name?
	config := ByWgConfig{Name: configName}
	scanner := bufio.NewScanner(file)

	var currentSection string
	var currentPeer wgtypes.PeerConfig = getEmptyPeer()
	interfaceCount := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// handle section headers
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			if currentSection == "peer" { // append currentPeer if previous section was a peer section
				config.Peers = append(config.Peers, currentPeer)
				currentPeer = getEmptyPeer()
			}

			currentSection = strings.ToLower(line[1 : len(line)-1])

			if currentSection == "interface" {
				interfaceCount++
				if interfaceCount > 1 {
					return ByWgConfig{}, fmt.Errorf("multiple [Interface] sections are not allowed")
				}
			}

			continue
		}

		// handle key-value pairs
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if idx := strings.Index(value, "#"); idx != -1 {
			value = strings.TrimSpace(value[:idx]) // remove inline comments
		}

		switch currentSection {
		case "interface":
			switch key {
			case "Address":
				addresses := strings.Split(value, ",")
				for _, addr := range addresses {
					addr = strings.TrimSpace(addr)
					if addr != "" {
						config.Address = append(config.Address, addr)
					}
				}
			case "ListenPort":
				port, err := strconv.Atoi(value)
				if err != nil {
					return ByWgConfig{}, fmt.Errorf("invalid ListenPort: %w", err)
				}
				config.ListenPort = &port
			case "PrivateKey":
				config.PrivateKey = value
			case "PreUp":
				config.PreUp = append(config.PreUp, value)
			case "PostUp":
				config.PostUp = append(config.PostUp, value)
			case "PreDown":
				config.PreDown = append(config.PreDown, value)
			case "PostDown":
				config.PostDown = append(config.PostDown, value)
			case "SaveConfig":
				config.SaveConfig = stringToBool(value)
			}
		case "peer":
			switch key {
			case "PublicKey":
				currentPeer.PublicKey, err = wgtypes.ParseKey(value)
				if err != nil {
					return ByWgConfig{}, fmt.Errorf("invalid peer PublicKey: %w", err)
				}
			case "PresharedKey":
				key, err := wgtypes.ParseKey(value)
				if err != nil {
					return ByWgConfig{}, fmt.Errorf("invalid peer PresharedKey: %w", err)
				}
				currentPeer.PresharedKey = &key
			case "Endpoint":
				currentPeer.Endpoint, err = net.ResolveUDPAddr("udp", value)
				if err != nil {
					return ByWgConfig{}, fmt.Errorf("invalid peer Endpoint: %w", err)
				}
			case "PersistentKeepaliveInterval":
				interval, err := time.ParseDuration(value)
				if err != nil {
					return ByWgConfig{}, fmt.Errorf("invalid peer PersistentKeepaliveInterval: %w", err)
				}
				currentPeer.PersistentKeepaliveInterval = &interval
			case "AllowedIPs":
				ips := strings.Split(value, ",")
				for _, ip := range ips {
					ip = strings.TrimSpace(ip)
					_, ipnet, err := net.ParseCIDR(ip)
					if err != nil {
						return ByWgConfig{}, fmt.Errorf("invalid peer AllowedIPs: %w", err)
					}
					currentPeer.AllowedIPs = append(currentPeer.AllowedIPs, *ipnet)
				}
			}
		}
	}

	if currentSection == "peer" { // append currentPeer if last section was a peer section
		config.Peers = append(config.Peers, currentPeer)
	}

	if err := scanner.Err(); err != nil {
		return ByWgConfig{}, fmt.Errorf("error reading file: %w", err)
	}

	if interfaceCount == 0 {
		return ByWgConfig{}, fmt.Errorf("[Interface] section must be specified")
	}

	if config.PrivateKey == "" {
		return ByWgConfig{}, fmt.Errorf("missing mandatory field PrivateKey")
	}

	for _, peer := range config.Peers {
		if peer.PublicKey == (wgtypes.Key{}) {
			return ByWgConfig{}, fmt.Errorf("each peer must have a PublicKey")
		}
	}

	return config, nil
}

// configToString transforms a ByWgConfig into a textual representation of the config (ini formatted).
func configToString(config ByWgConfig) string {
	var sb strings.Builder

	// write [Interface] section
	sb.WriteString("[Interface]\n")
	if len(config.Address) > 0 {
		sb.WriteString(fmt.Sprintf("Address = %s\n", strings.Join(config.Address, ", ")))
	}
	if config.ListenPort != nil {
		sb.WriteString(fmt.Sprintf("ListenPort = %d\n", *config.ListenPort))
	}
	if config.PrivateKey != "" { // should never be the case
		sb.WriteString(fmt.Sprintf("PrivateKey = %s\n", config.PrivateKey))
	}
	if config.SaveConfig {
		sb.WriteString("SaveConfig = true\n")
	}
	for _, cmd := range config.PreUp {
		sb.WriteString(fmt.Sprintf("PreUp = %s\n", cmd))
	}
	for _, cmd := range config.PostUp {
		sb.WriteString(fmt.Sprintf("PostUp = %s\n", cmd))
	}
	for _, cmd := range config.PreDown {
		sb.WriteString(fmt.Sprintf("PreDown = %s\n", cmd))
	}
	for _, cmd := range config.PostDown {
		sb.WriteString(fmt.Sprintf("PostDown = %s\n", cmd))
	}
	sb.WriteString("\n")

	// write [Peer] sections
	for _, peer := range config.Peers {
		sb.WriteString("[Peer]\n")
		sb.WriteString(fmt.Sprintf("PublicKey = %s\n", peer.PublicKey.String()))
		if peer.PresharedKey != nil && *peer.PresharedKey != (wgtypes.Key{}) {
			sb.WriteString(fmt.Sprintf("PresharedKey = %s\n", peer.PresharedKey.String()))
		}
		if len(peer.AllowedIPs) > 0 {
			sb.WriteString(fmt.Sprintf("AllowedIPs = %s\n", ipsString(peer.AllowedIPs)))
		}
		if peer.Endpoint != nil {
			sb.WriteString(fmt.Sprintf("Endpoint = %s\n", peer.Endpoint.String()))
		}
		if peer.PersistentKeepaliveInterval != nil && *peer.PersistentKeepaliveInterval != 0 {
			sb.WriteString(fmt.Sprintf("PersistentKeepaliveInterval = %s\n", peer.PersistentKeepaliveInterval.String()))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

func updateConfigFromDevice(config ByWgConfig, device *wgtypes.Device) ByWgConfig {
	config.PrivateKey = device.PrivateKey.String()
	config.ListenPort = &device.ListenPort
	config.Peers = getPeerConfigs(device.Peers)
	return config
}

// wgtypes.PeerConfig with several fields that should be set by default
func getEmptyPeer() wgtypes.PeerConfig {
	return wgtypes.PeerConfig{Remove: false, UpdateOnly: false, ReplaceAllowedIPs: true}
}

// convert a list of wgtypes.Peer to a list of wgtypes.PeerConfig.
func getPeerConfigs(peers []wgtypes.Peer) []wgtypes.PeerConfig {
	peerConfigs := []wgtypes.PeerConfig{}
	for _, peer := range peers {
		peerConfigs = append(peerConfigs, wgtypes.PeerConfig{
			PublicKey:                   peer.PublicKey,
			PresharedKey:                &peer.PresharedKey,
			Endpoint:                    peer.Endpoint,
			PersistentKeepaliveInterval: &peer.PersistentKeepaliveInterval,
			AllowedIPs:                  peer.AllowedIPs,
		})
	}
	return peerConfigs
}

// stringToBool returns true if the string is "true", "t", "yes", "y" or "1" (case insensitive), false otherwise.
func stringToBool(s string) bool {
	s = strings.TrimSpace(strings.ToLower(s))
	switch s {
	case "true", "t", "yes", "y", "1":
		return true
	}
	return false
}

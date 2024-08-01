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
	ListenPort *int
	PrivateKey string // mandatory
	PreUp      []string
	PostUp     []string
	PreDown    []string
	PostDown   []string
	Peers      []wgtypes.PeerConfig
}

func Test() {
	filePath := "/root/connect/tetherctl/bywg0.conf"

	config, err := ParseConfig(filePath)
	if err != nil {
		fmt.Printf("Error parsing config: %w\n", err)
		return
	}

	fmt.Printf("Parsed Config: %+v\n", config)
}

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

func getEmptyPeer() wgtypes.PeerConfig {
	return wgtypes.PeerConfig{Remove: false, UpdateOnly: false, ReplaceAllowedIPs: true}
}

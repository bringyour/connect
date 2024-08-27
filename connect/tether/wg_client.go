package tether

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"bringyour.com/wireguard/device"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

var (
	ErrDeviceExists   = errors.New("wireguard device already exists")
	ErrDeviceNotFound = errors.New("wireguard device not found")
)

// TODO: explain
type Client struct {
	devices    map[string]*device.Device
	DeviceName string // the current device's name
}

func New() *Client {
	return &Client{
		devices: make(map[string]*device.Device),
	}
}

func (c *Client) Close() error {
	return nil
}

func (c *Client) Devices() map[string]*device.Device {
	return c.devices
}

func (c *Client) Device(name string) (*wgtypes.Device, error) {
	device, ok := c.devices[name]
	if !ok {
		return nil, ErrDeviceNotFound
	}

	wgDevice, err := deviceTowgtypesDevice(device, name)
	if err != nil {
		return nil, fmt.Errorf("device %q converting to wgtypes.Device: %w", name, err)
	}
	return wgDevice, nil
}

func (c *Client) AddDevice(name string, wgDevice *device.Device) error {
	if _, ok := c.devices[name]; ok {
		return ErrDeviceExists
	}
	c.devices[name] = wgDevice
	return nil
}

func (c *Client) ConfigureDevice(name string, cfg wgtypes.Config) error {
	return nil
}

func parseHexKey(key string) (wgtypes.Key, error) {
	keyBytes, err := hex.DecodeString(key)
	if err != nil {
		return wgtypes.Key{}, err
	}

	wgKey, err := wgtypes.ParseKey(base64.StdEncoding.EncodeToString(keyBytes))
	if err != nil {
		return wgtypes.Key{}, err
	}
	return wgKey, nil
}

func deviceTowgtypesDevice(device *device.Device, name string) (*wgtypes.Device, error) {
	cfg, err := device.IpcGet()
	if err != nil {
		return nil, err
	}

	wgDevice := &wgtypes.Device{
		Name: name,
	}

	peers := make([]wgtypes.Peer, 0)
	var lastPeer *wgtypes.Peer

	lines := strings.Split(cfg, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			if lastPeer != nil {
				peers = append(peers, *lastPeer)
			}
			wgDevice.Peers = peers

			// empty line terminates operation
			return wgDevice, nil
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("invalid line in config: %s", line)
		}
		switch key {
		case "private_key":
			pk, err := parseHexKey(value)
			if err != nil {
				return nil, fmt.Errorf("parse private key: %w", err)
			}
			wgDevice.PrivateKey = pk
			wgDevice.PublicKey = pk.PublicKey()
		case "listen_port":
			port, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("parse listen port: %w", err)
			}
			wgDevice.ListenPort = port
		case "fwmark":
			mark, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("parse firewall mark: %w", err)
			}
			wgDevice.FirewallMark = mark
		case "public_key":
			if lastPeer != nil {
				peers = append(peers, *lastPeer)
			}

			pk, err := parseHexKey(value)
			if err != nil {
				return nil, fmt.Errorf("parse private key: %w", err)
			}
			lastPeer = &wgtypes.Peer{PublicKey: pk}
		default:
			err := handlePeerLine(lastPeer, key, value)
			if err != nil {
				return nil, err
			}
		}
	}
	return nil, errors.New("ipc get operation does not end in an empty line")
}

func handlePeerLine(peer *wgtypes.Peer, key string, value string) error {
	if peer == nil {
		return errors.New("trying to set a peer field without a peer")
	}
	switch key {
	case "preshared_key":
		pk, err := parseHexKey(value)
		if err != nil {
			return fmt.Errorf("parse preshared key: %w", err)
		}
		peer.PresharedKey = pk
	case "endpoint":
		endpoint, err := net.ResolveUDPAddr("udp", value)
		if err != nil {
			return fmt.Errorf("resolve endpoint: %w", err)
		}
		peer.Endpoint = endpoint
	case "persistent_keepalive_interval":
		// TODO: make sure here this is correctly gotten
		interval, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("parse persistent keepalive interval: %w", err)
		}
		peer.PersistentKeepaliveInterval = interval
	case "allowed_ip":
		_, ipNet, err := net.ParseCIDR(value)
		if err != nil {
			return fmt.Errorf("parse allowed ip: %w", err)
		}
		peer.AllowedIPs = append(peer.AllowedIPs, *ipNet)
	case "rx_bytes":
		// parse rx bytes as int64
		rxBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse rx bytes: %w", err)
		}
		peer.ReceiveBytes = rxBytes
	case "tx_bytes":
		txBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse tx bytes: %w", err)
		}
		peer.TransmitBytes = txBytes
	case "protocol_version":
		protocolVersion, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse protocol version: %w", err)
		}
		peer.ProtocolVersion = protocolVersion
	case "last_handshake_time_sec":
		fmt.Printf("last handshake time sec: %s\n", value)
	case "last_handshake_time_nsec":
		fmt.Printf("last handshake time nsec: %s\n", value)
	default:
		return errors.New(fmt.Sprintf("unknown key %q", key))
	}
	return nil
	// TODO: last handshake time
}

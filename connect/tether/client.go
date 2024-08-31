package tether

import (
	"fmt"

	"bringyour.com/wireguard/device"
	"bringyour.com/wireguard/tun"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

type Device struct {
	*device.Device
	Addresses []string // list of addresses peers can have on the device
}

// A Client manages different userspace-wireguard devices.
//
// Based on wgctrl.Client
type Client struct {
	devices map[string]*Device
}

// New creates a new Client.
func New(deviceName string) *Client {
	return &Client{
		devices: make(map[string]*Device),
	}
}

// Close closes all devices managed by the client and removes them from the client.
func (c *Client) Close() {
	for _, device := range c.devices {
		device.Close()
	}
	c.devices = make(map[string]*Device)
}

// Devices return the list of userspace-wireguard devices managed by the client.
func (c *Client) Devices() map[string]*Device {
	return c.devices
}

// GetAvailableDevices returns a list of the names of all available devices on the client.
func (c *Client) GetAvailableDevices() []string {
	deviceNames := make([]string, 0, len(c.devices))
	for name := range c.devices {
		deviceNames = append(deviceNames, name)
	}
	return deviceNames
}

// Device returns a userspace-wireguard device by its name in the format of a wgtypes.Device.
//
// If the device by the same name does not exist or some information
// of the device could not be serialized, an error is returned.
// The first error can be checked using errors.Is(err, ErrDeviceNotFound).
func (c *Client) Device(name string) (*wgtypes.Device, error) {
	device, ok := c.devices[name]
	if !ok {
		return nil, fmt.Errorf("device %s: %w", name, ErrDeviceNotFound)
	}

	wgDevice, err := device.IpcGet()
	if err != nil {
		return nil, fmt.Errorf("device %q ipc error: %w", name, err)
	}
	wgDevice.Name = name
	return wgDevice, nil
}

// AddDevice adds a new userspace-wireguard device to the client with the provided name, device and addresses.
// The name of the device must be unique to the other devices on the Client.
//
// Returns an error if the device by the same name already exists or if any of the addresses are not valid CIDR network addresses.
// The errors can be checked using errors.Is(err, ErrDeviceExists | ErrInvalidAddress | ErrNotNetworkAddress), respectively.
func (c *Client) AddDevice(name string, wgDevice *device.Device, addresses []string) error {
	if _, ok := c.devices[name]; ok {
		return fmt.Errorf("device %s: %w", name, ErrDeviceExists)
	}
	for _, addr := range addresses {
		if err := isNetworkAddress(addr); err != nil {
			return err
		}
	}
	c.devices[name] = &Device{Device: wgDevice, Addresses: addresses}
	return nil
}

// RemoveDevice safely removes a userspace-wireguard device from the client.
//
// If the device by the same name does not exist an error is returned which can be checked using errors.Is(err, ErrDeviceNotFound).
func (c *Client) RemoveDevice(name string) error {
	device, ok := c.devices[name]
	if !ok {
		return fmt.Errorf("device %s: %w", name, ErrDeviceNotFound)
	}

	device.Close()
	delete(c.devices, name)
	return nil
}

// ConfigureDevice configures a device by its name with the provided Config.
//
// Because the zero value of some Go types may be significant to WireGuard for
// Config fields, only fields which are not nil will be applied when
// configuring a device.
//
// If the device specified by name does not exist or the configuration
// could not be applied, an error is returned.
// The first error can be checked using errors.Is(err, ErrDeviceNotFound).
func (c *Client) ConfigureDevice(name string, cfg wgtypes.Config) error {
	device, ok := c.devices[name]
	if !ok {
		return fmt.Errorf("device %s: %w", name, ErrDeviceNotFound)
	}

	return device.IpcSet(&cfg)
}

// AddEventToDevice adds an event to a device by its name.
//
// If the device does not exist an error is returned which can be checked using errors.Is(err, ErrDeviceNotFound).
func (c *Client) AddEventToDevice(name string, event tun.Event) error {
	device, ok := c.devices[name]
	if !ok {
		return fmt.Errorf("device %s: %w", name, ErrDeviceNotFound)
	}
	device.AddEvent(event)
	return nil
}

// AddAddressesToDevice adds a list of addresses to a device by its name.
//
// replace specifies if the addresses should replace the existing addresses, instead of appending them to the existing addresses.
//
// Returns an error if the device by the same name does not exist or if any of the addresses are not valid CIDR network addresses.
// The errors can be checked using errors.Is(err, ErrDeviceNotFound | ErrInvalidAddress | ErrNotNetworkAddress), respectively.
func (c *Client) AddAddressesToDevice(name string, addresses []string, replace bool) error {
	device, ok := c.devices[name]
	if !ok {
		return fmt.Errorf("device %s: %w", name, ErrDeviceNotFound)
	}

	for _, addr := range addresses {
		if err := isNetworkAddress(addr); err != nil {
			return err
		}
	}

	if replace {
		device.Addresses = addresses
		return nil
	}

	device.Addresses = append(device.Addresses, addresses...)
	return nil
}

// GetAddressesFromDevice returns a list of addresses from a device by its name.
//
// If the device by the same name does not exist an error is returned which can be checked using errors.Is(err, ErrDeviceNotFound).
func (c *Client) GetAddressesFromDevice(name string) ([]string, error) {
	device, ok := c.devices[name]
	if !ok {
		return nil, fmt.Errorf("device %s: %w", name, ErrDeviceNotFound)
	}
	return device.Addresses, nil
}

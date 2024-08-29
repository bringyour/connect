package tether

import (
	"fmt"

	"bringyour.com/wireguard/device"
	"bringyour.com/wireguard/tun"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

func ErrDeviceExists(deviceName string) error {
	return fmt.Errorf("wireguard device %q already exists", deviceName)
}

func ErrDeviceNotFound(deviceName string) error {
	return fmt.Errorf("wireguard device %q not found", deviceName)
}

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
func (c *Client) Device(name string) (*wgtypes.Device, error) {
	device, ok := c.devices[name]
	if !ok {
		return nil, ErrDeviceNotFound(name)
	}

	wgDevice, err := device.IpcGet()
	if err != nil {
		return nil, fmt.Errorf("device %q ipc error: %w", name, err)
	}
	wgDevice.Name = name
	return wgDevice, nil
}

// AddDevice adds a new userspace-wireguard device to the client with the provided name, device and addresses.
// The name of the device must be unique.
//
// If the device by the same name already exists, an error is returned.
func (c *Client) AddDevice(name string, wgDevice *device.Device, addresses []string) error {
	if _, ok := c.devices[name]; ok {
		return ErrDeviceExists(name)
	}
	c.devices[name] = &Device{Device: wgDevice, Addresses: addresses}
	return nil
}

// RemoveDevice safely removes a userspace-wireguard device from the client.
//
// If the device by the same name does not exist, an error is returned.
func (c *Client) RemoveDevice(name string) error {
	device, ok := c.devices[name]
	if !ok {
		return ErrDeviceNotFound(name)
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
func (c *Client) ConfigureDevice(name string, cfg wgtypes.Config) error {
	device, ok := c.devices[name]
	if !ok {
		return ErrDeviceNotFound(name)
	}

	return device.IpcSet(&cfg)
}

// AddEventToDevice adds an event to a device by its name.
func (c *Client) AddEventToDevice(name string, event tun.Event) error {
	device, ok := c.devices[name]
	if !ok {
		return ErrDeviceNotFound(name)
	}
	device.AddEvent(event)
	return nil
}

// AddAddressesToDevice adds a list of addresses to a device by its name.
//
// replace specifies if the addresses should replace the existing addresses, instead of appending them to the existing addresses.
//
// If the device by the same name does not exist, an error is returned.
func (c *Client) AddAddressesToDevice(name string, addresses []string, replace bool) error {
	device, ok := c.devices[name]
	if !ok {
		return ErrDeviceNotFound(name)
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
// If the device by the same name does not exist, an error is returned.
func (c *Client) GetAddressesFromDevice(name string) ([]string, error) {
	device, ok := c.devices[name]
	if !ok {
		return nil, ErrDeviceNotFound(name)
	}
	return device.Addresses, nil
}

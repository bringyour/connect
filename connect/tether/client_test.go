package tether

import (
	"errors"
	"reflect"
	"testing"

	"bringyour.com/wireguard/tun"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// Mock implementation of the IDevice interface
type mockDevice struct {
	name         string
	addresses    []string
	closed       bool
	ipcGetCalled bool
	ipcGetErr    error
	ipcSetCalled bool
	ipcSetErr    error
	eventAdded   bool
	ipcGetPeers  []wgtypes.Peer
}

func (m *mockDevice) Close() { m.closed = true }
func (m *mockDevice) IpcGet() (*wgtypes.Device, error) {
	m.ipcGetCalled = true
	if m.ipcGetErr != nil {
		return nil, m.ipcGetErr
	}
	return &wgtypes.Device{
		Name:  m.name,
		Peers: m.ipcGetPeers,
	}, nil
}
func (m *mockDevice) IpcSet(cfg *wgtypes.Config) error {
	m.ipcSetCalled = true
	return m.ipcSetErr
}
func (m *mockDevice) AddEvent(event tun.Event) { m.eventAdded = true }
func (d *mockDevice) GetAddresses() []string   { return d.addresses }
func (d *mockDevice) SetAddresses(addresses []string, replace bool) {
	if replace {
		d.addresses = addresses
		return
	}
	d.addresses = append(d.addresses, addresses...)
}

func TestClientNew(t *testing.T) {
	c := New()
	if c == nil {
		t.Fatalf("New() returned nil")
	}
	if len(c.devices) != 0 {
		t.Fatalf("New() did not initialize devices map")
	}
}

func TestClientAddDevice(t *testing.T) {
	testCases := []struct {
		name    string
		device  IDevice
		wantErr error
	}{
		{
			name:    "Add new device",
			device:  &mockDevice{name: "bywg0"},
			wantErr: nil,
		},
		{
			name:    "Add existing device",
			device:  &mockDevice{name: "bywg0"},
			wantErr: ErrDeviceExists,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			if tc.name == "Add existing device" {
				// Add the device first to simulate an existing device
				err := c.AddDevice("bywg0", tc.device)
				if err != nil {
					t.Fatalf("failed to add test device: %v", err)
				}
			}

			err := c.AddDevice("bywg0", tc.device)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("AddDevice() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if err == nil {
				if _, ok := c.devices["bywg0"]; !ok {
					t.Errorf("device not added to client")
				}
			}
		})
	}
}

func TestClientRemoveDevice(t *testing.T) {
	testCases := []struct {
		name    string
		device  *mockDevice
		wantErr error
	}{
		{
			name:    "Remove existing device",
			device:  &mockDevice{name: "bywg0"},
			wantErr: nil,
		},
		{
			name:    "Remove non-existent device",
			device:  nil,
			wantErr: ErrDeviceNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			if tc.device != nil {
				err := c.AddDevice("bywg0", tc.device)
				if err != nil {
					t.Fatalf("failed to add test device: %v", err)
				}
			}
			err := c.RemoveDevice("bywg0")
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("RemoveDevice() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if err != nil {
				return // if we expected an error do not check further
			}
			if tc.device != nil {
				if !tc.device.closed {
					t.Errorf("device not closed")
				}
			}
			if _, stillThere := c.devices["bywg0"]; stillThere {
				t.Errorf("device not removed from client")
			}
		})
	}
}

func TestClientClose(t *testing.T) {
	// Create a client with some mock devices
	c := New()
	mockDevice1 := &mockDevice{name: "device1"}
	mockDevice2 := &mockDevice{name: "device2"}
	c.AddDevice("device1", mockDevice1)
	c.AddDevice("device2", mockDevice2)

	c.Close()

	if len(c.devices) != 0 {
		t.Errorf("devices map should be empty after Close()")
	}
	if !mockDevice1.closed {
		t.Errorf("device1 should be closed after Close()")
	}
	if !mockDevice2.closed {
		t.Errorf("device2 should be closed after Close()")
	}
}

func TestClientDevices(t *testing.T) {
	// Create a client with some mock devices
	c := New()
	mockDevice1 := &mockDevice{name: "device1"}
	mockDevice2 := &mockDevice{name: "device2"}
	c.AddDevice("device1", mockDevice1)
	c.AddDevice("device2", mockDevice2)

	devices := c.Devices()
	if len(devices) != 2 {
		t.Errorf("Expected 2 devices, got %d", len(devices))
	}

	if _, ok := devices["device1"]; !ok {
		t.Errorf("device1 not found in the returned map")
	}
	if _, ok := devices["device2"]; !ok {
		t.Errorf("device2 not found in the returned map")
	}
}

func TestClientGetAvailableDevices(t *testing.T) {
	c := New()
	c.AddDevice("device1", &mockDevice{name: "device1"})
	c.AddDevice("device2", &mockDevice{name: "device2"})

	deviceNames := c.GetAvailableDevices()

	expectedDeviceNames := []string{"device1", "device2"}
	if !reflect.DeepEqual(deviceNames, expectedDeviceNames) {
		t.Errorf("Devices() returned unexpected device names:\nGot: %v\nWant: %v", deviceNames, expectedDeviceNames)
	}
}

func TestClientAddAddressesToDevice(t *testing.T) {
	testCases := []struct {
		name       string
		deviceName string
		device     IDevice
		addresses  []string
		replace    bool
		wantErr    error
		wantAddrs  []string // expected addresses after the operation
	}{
		{
			name:       "Add addresses to existing device (append)",
			deviceName: "bywg0",
			device:     &mockDevice{name: "bywg0", addresses: []string{"10.0.0.1/32"}},
			addresses:  []string{"192.168.1.2/32", "fd00::1/128"},
			replace:    false, // Append
			wantErr:    nil,
			wantAddrs:  []string{"10.0.0.1/32", "192.168.1.2/32", "fd00::1/128"},
		},
		{
			name:       "Add addresses to existing device (replace)",
			deviceName: "bywg0",
			device:     &mockDevice{name: "bywg0", addresses: []string{"10.0.0.1/32"}},
			addresses:  []string{"192.168.1.2/32", "fd00::1/128"},
			replace:    true, // Replace
			wantErr:    nil,
			wantAddrs:  []string{"192.168.1.2/32", "fd00::1/128"},
		},
		{
			name:       "Add addresses to non-existent device",
			deviceName: "nonexistent",
			device:     nil,
			addresses:  []string{"10.0.0.1/32"},
			replace:    false,
			wantErr:    ErrDeviceNotFound,
		},
		{
			name:       "Add invalid address",
			deviceName: "bywg0",
			device:     &mockDevice{name: "bywg0"},
			addresses:  []string{"invalid"},
			replace:    false,
			wantErr:    ErrInvalidAddress,
		},
		{
			name:       "Add non-network address",
			deviceName: "bywg0",
			device:     &mockDevice{name: "bywg0"},
			addresses:  []string{"10.0.0.1/24"},
			replace:    false,
			wantErr:    ErrNotNetworkAddress,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := New()
			if tc.device != nil {
				err := c.AddDevice(tc.deviceName, tc.device)
				if err != nil {
					t.Fatalf("failed to add test device: %v", err)
				}
			}

			err := c.AddAddressesToDevice(tc.deviceName, tc.addresses, tc.replace)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("AddAddressesToDevice(%s, %s, %v) error = %T, wantErr %T,", tc.deviceName, tc.addresses, tc.replace, err, tc.wantErr)
				return
			}

			if err == nil {
				device, ok := c.devices[tc.deviceName]
				if !ok {
					t.Errorf("device not found after adding addresses")
					return
				}
				if !reflect.DeepEqual(device.GetAddresses(), tc.wantAddrs) {
					t.Errorf("incorrect addresses after AddAddressesToDevice:\nGot: %v\nWant: %v", device.GetAddresses(), tc.wantAddrs)
				}
			}
		})
	}
}

func TestClientGetAddressesFromDevice(t *testing.T) {
	testCases := []struct {
		name       string
		deviceName string
		device     IDevice
		wantAddrs  []string
		wantErr    error
	}{
		{
			name:       "Get addresses from existing device",
			deviceName: "bywg0",
			device:     &mockDevice{name: "bywg0", addresses: []string{"10.0.0.1/32", "fd00::1/64"}},
			wantAddrs:  []string{"10.0.0.1/32", "fd00::1/64"},
			wantErr:    nil,
		},
		{
			name:       "Get addresses from non-existent device",
			deviceName: "nonexistent",
			device:     nil,
			wantErr:    ErrDeviceNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			if tc.device != nil {
				err := c.AddDevice(tc.deviceName, tc.device)
				if err != nil {
					t.Fatalf("failed to add test device: %v", err)
				}
			}

			addrs, err := c.GetAddressesFromDevice(tc.deviceName)

			if !errors.Is(err, tc.wantErr) {
				t.Errorf("GetAddressesFromDevice() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if err == nil && !reflect.DeepEqual(addrs, tc.wantAddrs) {
				t.Errorf("GetAddressesFromDevice() returned incorrect addresses:\nGot: %v\nWant: %v", addrs, tc.wantAddrs)
			}
		})
	}
}

func TestClientAddEventToDevice(t *testing.T) {
	testCases := []struct {
		name               string
		deviceName         string
		device             *mockDevice
		event              tun.Event
		wantErr            error
		expectAddEventCall bool // flag to check if AddEvent should be called
	}{
		{
			name:               "Add event to existing device",
			deviceName:         "bywg0",
			device:             &mockDevice{name: "bywg0"},
			event:              tun.EventUp, // Example event
			wantErr:            nil,
			expectAddEventCall: true,
		},
		{
			name:               "Add event to non-existent device",
			deviceName:         "nonexistent",
			device:             nil,
			event:              tun.EventUp,
			wantErr:            ErrDeviceNotFound,
			expectAddEventCall: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			if tc.device != nil {
				err := c.AddDevice(tc.deviceName, tc.device)
				if err != nil {
					t.Fatalf("failed to add test device: %v", err)
				}
			}

			err := c.AddEventToDevice(tc.deviceName, tc.event)

			if !errors.Is(err, tc.wantErr) {
				t.Errorf("AddEventToDevice() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.device != nil {
				if !tc.device.eventAdded {
					t.Errorf("AddEvent not called on device")
				}
			}
		})
	}
}

func TestClientConfigureDevice(t *testing.T) {
	errorIpcSet := errors.New("ipc set error")

	testCases := []struct {
		name         string
		deviceName   string
		device       *mockDevice
		wantErr      error
		expectIpcSet bool
	}{
		{
			name:         "Configure existing device (success)",
			deviceName:   "bywg0",
			device:       &mockDevice{},
			wantErr:      nil,
			expectIpcSet: true,
		},
		{
			name:         "Configure non-existent device",
			deviceName:   "nonexistent",
			device:       nil,
			wantErr:      ErrDeviceNotFound,
			expectIpcSet: false,
		},
		{
			name:         "Configure existing device with IpcSet error",
			deviceName:   "bywg0",
			device:       &mockDevice{ipcSetErr: errorIpcSet},
			wantErr:      errorIpcSet,
			expectIpcSet: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			if tc.device != nil {
				err := c.AddDevice(tc.deviceName, tc.device)
				if err != nil {
					t.Fatalf("failed to add test device: %v", err)
				}
			}

			err := c.ConfigureDevice(tc.deviceName, wgtypes.Config{})

			if !errors.Is(err, tc.wantErr) {
				t.Errorf("ConfigureDevice() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.device != nil {
				if tc.device.ipcSetCalled != tc.expectIpcSet {
					t.Errorf("IpcSet call expectation mismatch. Expected: %v, Actual: %v", tc.expectIpcSet, tc.device.ipcSetCalled)
				}
			}
		})
	}
}

func TestClientDevice(t *testing.T) {
	errorIpcGet := errors.New("ipc get error")

	testCases := []struct {
		name         string
		deviceName   string
		device       *mockDevice
		wantErr      error
		expectIpcGet bool
	}{
		{
			name:         "Get existing device",
			deviceName:   "bywg0",
			device:       &mockDevice{name: "bywg0"},
			wantErr:      nil,
			expectIpcGet: true,
		},
		{
			name:         "Get non-existent device",
			deviceName:   "nonexistent",
			device:       nil,
			wantErr:      ErrDeviceNotFound,
			expectIpcGet: false,
		},
		{
			name:         "Get existing device with IpcGet error",
			deviceName:   "bywg0",
			device:       &mockDevice{name: "bywg0", ipcGetErr: errorIpcGet},
			wantErr:      errorIpcGet,
			expectIpcGet: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			if tc.device != nil {
				err := c.AddDevice(tc.deviceName, tc.device)
				if err != nil {
					t.Fatalf("failed to add test device: %v", err)
				}
			}

			wgDevice, err := c.Device(tc.deviceName)

			if !errors.Is(err, tc.wantErr) {
				t.Errorf("Device() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			// check if IpcGet was called on the mock device
			if tc.device != nil {
				if tc.device.ipcGetCalled != tc.expectIpcGet {
					t.Errorf("IpcGet call expectation mismatch. Expected: %v, Actual: %v", tc.expectIpcGet, tc.device.ipcGetCalled)
				}
			}

			// if no error, check the returned device
			if err == nil {
				if wgDevice == nil {
					t.Errorf("Device() returned nil device")
					return
				}
				if wgDevice.Name != tc.deviceName {
					t.Errorf("Device() returned incorrect device name. Got: %s, Want: %s", wgDevice.Name, tc.deviceName)
				}
			}
		})
	}
}

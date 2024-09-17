package tether

import (
	"errors"
	"net"
	"reflect"
	"testing"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

func mustCIDR(s string) net.IPNet {
	_, ipnet, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return *ipnet
}

func TestIpsString(t *testing.T) {
	tests := []struct {
		name    string
		ipnets  []net.IPNet
		want    string
		wantErr bool
	}{
		{
			name:   "Empty slice",
			ipnets: []net.IPNet{},
			want:   "",
		},
		{
			name: "Single IPv4",
			ipnets: []net.IPNet{
				mustCIDR("192.168.1.0/24"),
			},
			want: "192.168.1.0/24",
		},
		{
			name: "Multiple IPv4",
			ipnets: []net.IPNet{
				mustCIDR("192.168.1.0/24"),
				mustCIDR("10.0.0.0/8"),
			},
			want: "192.168.1.0/24, 10.0.0.0/8",
		},
		{
			name: "Single IPv6",
			ipnets: []net.IPNet{
				mustCIDR("2001:db8::/32"),
			},
			want: "2001:db8::/32",
		},
		{
			name: "Mixed IPv4 and IPv6",
			ipnets: []net.IPNet{
				mustCIDR("192.168.1.0/24"),
				mustCIDR("2001:db8::/32"),
			},
			want: "192.168.1.0/24, 2001:db8::/32",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ipsString(tt.ipnets)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ipsString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBringUpDevice(t *testing.T) {
	errIpcSet := errors.New("ipc set error")
	errIpcGet := errors.New("ipc get error")
	privateKey := "IGSnyffcEc7HLIjG9TzHLDnir1p265lo89DKX5FVsWM="

	testCases := []struct {
		name           string
		deviceName     string
		device         *mockDevice
		config         ByWgConfig
		wantErr        error // expected error type (if unknown or unexpected set to nil)
		wantErrUnknown bool  // we expect an error but dont know the type
		expectIpcSet   bool
		expectAddEvent bool
		wantAddrs      []string
	}{
		{
			name:           "Bring up existing device (success)",
			deviceName:     "bywg0",
			device:         &mockDevice{name: "bywg0"},
			config:         ByWgConfig{Name: "bywg0", PrivateKey: privateKey, Address: []string{"10.0.0.1/32", "fd00::1/128"}},
			wantErr:        nil,
			expectIpcSet:   true,
			expectAddEvent: true,
			wantAddrs:      []string{"10.0.0.1/32", "fd00::1/128"},
		},
		{
			name:           "Bring up non-existent device",
			deviceName:     "nonexistent",
			device:         nil,
			config:         ByWgConfig{Name: "nonexistent"},
			wantErr:        ErrDeviceNotFound,
			expectIpcSet:   false,
			expectAddEvent: false,
		},
		{
			name:           "Bring up device with name mismatch in config",
			deviceName:     "bywg0",
			device:         &mockDevice{name: "bywg0"},
			config:         ByWgConfig{Name: "bywg1", PrivateKey: privateKey}, // mismatch
			wantErr:        ErrNameMismatch,
			expectIpcSet:   false,
			expectAddEvent: false,
		},
		{
			name:           "Bring up device with invalid private key",
			deviceName:     "bywg0",
			device:         &mockDevice{name: "bywg0"},
			config:         ByWgConfig{Name: "bywg0", PrivateKey: "invalid_key"},
			wantErr:        nil,
			wantErrUnknown: true,
			expectIpcSet:   false,
			expectAddEvent: false,
		},
		{
			name:           "Bring up device with IpcSet error",
			deviceName:     "bywg0",
			device:         &mockDevice{name: "bywg0", ipcSetErr: errIpcSet},
			config:         ByWgConfig{Name: "bywg0", PrivateKey: privateKey},
			wantErr:        errIpcSet,
			expectIpcSet:   true,
			expectAddEvent: false, // AddEvent shouldn't be called if IpcSet fails
		},
		{
			name:           "Bring up device with IpcGet error",
			deviceName:     "bywg0",
			device:         &mockDevice{name: "bywg0", ipcGetErr: errIpcGet},
			config:         ByWgConfig{Name: "bywg0", PrivateKey: privateKey},
			wantErr:        errIpcGet,
			expectIpcSet:   false,
			expectAddEvent: false,
		},
		{
			name:           "Bring up device with invalid addresses",
			deviceName:     "bywg0",
			device:         &mockDevice{name: "bywg0"},
			config:         ByWgConfig{Name: "bywg0", PrivateKey: privateKey, Address: []string{"invalid"}},
			wantErr:        nil,
			wantErrUnknown: true,
			expectIpcSet:   true,
			expectAddEvent: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := New()
			if tc.device != nil {
				// Add the device to the client if it exists
				err := c.AddDevice(tc.deviceName, tc.device)
				if err != nil {
					t.Fatalf("failed to add test device: %v", err)
				}
			}

			err := c.BringUpDevice(tc.deviceName, tc.config)

			if (tc.wantErrUnknown != (err != nil)) && tc.wantErr == nil {
				t.Fatalf("BringUpDevice() hasError = %v, wantErrUnknown %v", (err != nil), tc.wantErrUnknown)
			}

			if !errors.Is(err, tc.wantErr) && !tc.wantErrUnknown {
				t.Fatalf("BringUpDevice() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.device != nil {
				// check if IpcSet and AddEvent were called as expected
				if tc.device.ipcSetCalled != tc.expectIpcSet {
					t.Fatalf("IpcSet call expectation mismatch. Expected: %v, Actual: %v", tc.expectIpcSet, tc.device.ipcSetCalled)
				}
				if tc.device.eventAdded != tc.expectAddEvent {
					t.Fatalf("AddEvent call expectation mismatch. Expected: %v, Actual: %v", tc.expectAddEvent, tc.device.eventAdded)
				}
				if !reflect.DeepEqual(tc.device.GetAddresses(), tc.wantAddrs) {
					t.Fatalf("incorrect addresses after BringUpDevice:\nGot: %v\nWant: %v", tc.device.GetAddresses(), tc.wantAddrs)
				}
			}
		})
	}
}

func TestClientBringDownDevice(t *testing.T) {
	errIpcGet := errors.New("ipc get error")
	privateKey := "IGSnyffcEc7HLIjG9TzHLDnir1p265lo89DKX5FVsWM="

	testCases := []struct {
		name            string
		deviceName      string
		device          *mockDevice
		config          ByWgConfig
		wantErr         error
		expectAddEvent  bool
		expectIpcGetErr bool
		wantAddrs       []string
	}{
		{
			name:           "Bring down existing device (success)",
			deviceName:     "bywg0",
			device:         &mockDevice{name: "bywg0"},
			config:         ByWgConfig{Name: "bywg0", PrivateKey: privateKey, Address: []string{"10.0.0.1/32", "fd00::1/128"}},
			wantErr:        nil,
			expectAddEvent: true,
			wantAddrs:      []string{"10.0.0.1/32", "fd00::1/128"},
		},
		{
			name:           "Bring down non-existent device",
			deviceName:     "nonexistent",
			device:         nil,
			config:         ByWgConfig{Name: "nonexistent"},
			wantErr:        ErrDeviceNotFound,
			expectAddEvent: false,
		},
		{
			name:           "Bring down device with name mismatch in config",
			deviceName:     "bywg0",
			device:         &mockDevice{name: "bywg0"},
			config:         ByWgConfig{Name: "bywg1", PrivateKey: privateKey}, // mismatch
			wantErr:        ErrNameMismatch,
			expectAddEvent: false,
		},
		{
			name:            "Bring down device with IpcGet error",
			deviceName:      "bywg0",
			device:          &mockDevice{name: "bywg0"},
			config:          ByWgConfig{Name: "bywg0", PrivateKey: privateKey},
			wantErr:         errIpcGet,
			expectIpcGetErr: true,
			expectAddEvent:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := New()
			if tc.device != nil {
				// Add the device to the client if it exists
				err := c.AddDevice(tc.deviceName, tc.device)
				if err != nil {
					t.Fatalf("failed to add test device: %v", err)
				}
				newConf := tc.config
				newConf.Name = tc.deviceName
				err = c.BringUpDevice(tc.deviceName, newConf)
				if err != nil {
					t.Fatalf("failed to bring up test device: %v", err)
				}
				tc.device.eventAdded = false
			}
			if tc.expectIpcGetErr {
				tc.device.ipcGetErr = errIpcGet
			}

			err := c.BringDownDevice(tc.deviceName, tc.config, "")

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("BringDownDevice() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.device != nil {
				if tc.device.eventAdded != tc.expectAddEvent {
					t.Fatalf("AddEvent call expectation mismatch. Expected: %v, Actual: %v", tc.expectAddEvent, tc.device.eventAdded)
				}
				if !reflect.DeepEqual(tc.device.GetAddresses(), tc.wantAddrs) {
					t.Fatalf("incorrect addresses after BringDownDevice:\nGot: %v\nWant: %v", tc.device.GetAddresses(), tc.wantAddrs)
				}
			}
		})
	}
}

func TestClientAddPeerToDevice(t *testing.T) {
	peerPublicKey1, _ := wgtypes.ParseKey("HVK72pOt92DtiXAU2WP9wh0ZUsCUTG40PcY12ibgRwI=")
	errIpcSet := errors.New("ipc set error")

	testCases := []struct {
		name           string
		deviceName     string
		device         *mockDevice
		pubKey         wgtypes.Key
		wantErr        error // expected error type (if unknown or unexpected set to nil)
		wantErrUnknown bool  // we expect an error but dont know the type
		expectIpcSet   bool
	}{
		{
			name:         "Add peer to existing device (success)",
			deviceName:   "bywg0",
			device:       &mockDevice{name: "bywg0", addresses: []string{"10.0.0.0/24"}},
			pubKey:       peerPublicKey1,
			wantErr:      nil,
			expectIpcSet: true,
		},
		{
			name:         "Add peer to non-existent device",
			deviceName:   "nonexistent",
			device:       nil,
			pubKey:       peerPublicKey1,
			wantErr:      ErrDeviceNotFound,
			expectIpcSet: false,
		},
		{
			name:       "Add existing peer",
			deviceName: "bywg0",
			device: &mockDevice{
				name:        "bywg0",
				addresses:   []string{"10.0.0.0/24"},
				ipcGetPeers: []wgtypes.Peer{{PublicKey: peerPublicKey1}},
			},
			pubKey:         peerPublicKey1,
			wantErr:        nil,
			wantErrUnknown: true,
			expectIpcSet:   false,
		},
		{
			name:       "Add peer to device with no addresses",
			deviceName: "bywg0",
			device: &mockDevice{
				name: "bywg0",
			},
			pubKey:       peerPublicKey1,
			wantErr:      ErrorAIPsNoAddressesFound,
			expectIpcSet: false,
		},
		{
			name:       "Add peer to device with no space (full subnet)",
			deviceName: "bywg0",
			device: &mockDevice{
				name:      "bywg0",
				addresses: []string{"10.0.0.0/32"}, // Simulate a full subnet
			},
			pubKey:       peerPublicKey1,
			wantErr:      ErrorAIPsNoAvailableIP,
			expectIpcSet: false,
		},
		{
			name:       "Add peer with IpcSet error",
			deviceName: "bywg0",
			device: &mockDevice{
				name:      "bywg0",
				ipcSetErr: errIpcSet,
				addresses: []string{"10.0.0.0/24"},
			},
			pubKey:       peerPublicKey1,
			wantErr:      errIpcSet,
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

			err := c.AddPeerToDevice(tc.deviceName, tc.pubKey)

			if (tc.wantErrUnknown != (err != nil)) && tc.wantErr == nil {
				t.Fatalf("AddPeerToDevice() hasError = %v, wantErrUnknown %v", (err != nil), tc.wantErrUnknown)
			}

			if !errors.Is(err, tc.wantErr) && !tc.wantErrUnknown {
				t.Fatalf("AddPeerToDevice() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.device != nil {
				if tc.device.ipcSetCalled != tc.expectIpcSet {
					t.Fatalf("IpcSet call expectation mismatch. Expected: %v, Actual: %v", tc.expectIpcSet, tc.device.ipcSetCalled)
				}
			}
		})
	}
}

func TestClient_RemovePeerFromDevice(t *testing.T) {
	peerPublicKey1, _ := wgtypes.ParseKey("HVK72pOt92DtiXAU2WP9wh0ZUsCUTG40PcY12ibgRwI=")
	errIpcSet := errors.New("ipc set error")

	testCases := []struct {
		name         string
		deviceName   string
		device       *mockDevice
		pubKey       wgtypes.Key
		wantErr      error
		expectIpcSet bool
	}{
		{
			name:         "Remove existing peer (success)",
			deviceName:   "wg0",
			device:       &mockDevice{name: "wg0", ipcGetPeers: []wgtypes.Peer{{PublicKey: peerPublicKey1}}},
			pubKey:       peerPublicKey1,
			wantErr:      nil,
			expectIpcSet: true,
		},
		{
			name:         "Remove non-existent peer",
			deviceName:   "wg0",
			device:       &mockDevice{name: "wg0"},
			pubKey:       peerPublicKey1,
			wantErr:      nil,  // No error expected if peer doesn't exist
			expectIpcSet: true, // IpcSet should still be called to attempt removal
		},
		{
			name:         "Remove peer with IpcSet error",
			deviceName:   "wg0",
			device:       &mockDevice{name: "wg0", ipcSetErr: errIpcSet},
			pubKey:       peerPublicKey1,
			wantErr:      errIpcSet,
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

			err := c.RemovePeerFromDevice(tc.deviceName, tc.pubKey)

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("RemovePeerFromDevice() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.device != nil {
				if tc.device.ipcSetCalled != tc.expectIpcSet {
					t.Fatalf("IpcSet call expectation mismatch. Expected: %v, Actual: %v", tc.expectIpcSet, tc.device.ipcSetCalled)
				}
			}
		})
	}
}

package tether

import "errors"

// errors.go provides all custom error types for the tether package
//
// error type checking:
//   an error can be checked if it is any of these using errors.Is(err, ErrType)

// used for allowed ips
var (
	ErrorAIPsNoAddressesFound = errors.New("no addresses found")
	ErrorAIPsNoAvailableIP    = errors.New("no available IP found")
)

// used for client devices
var (
	ErrDeviceNotFound = errors.New("wireguard device not found")
	ErrDeviceExists   = errors.New("wireguard device already exists")
)

// used for addresses of a device
var (
	ErrInvalidAddress    = errors.New("invalid address")
	ErrNotNetworkAddress = errors.New("address must be network address")
)

// used for config
var (
	ErrNameMismatch = errors.New("name in config does not match the device name")
)

// used for endpoints of client
var (
	ErrInvalidEndpointType = errors.New("invalid endpoint type")
	ErrEndpointNotFound    = errors.New("endpoint not found")
	ErrEndpointAddAny      = errors.New("cannot add endpoint of type any")
)

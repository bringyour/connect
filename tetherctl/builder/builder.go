package builder

import (
	"bringyour.com/connect/tether"
	"bringyour.com/tetherctl/api"
	"bringyour.com/wireguard/logger"
)

type EndpointOptions struct {
	EndpointType tether.EndpointType
	Endpoint     string
	Remove       bool // by default endpoint is added unless remove is set
	Reset        bool // by default endpoint is added unless reset is set
}

type PeerOptions struct {
	PubKey       string
	Remove       bool                // by default peer is added unless remove is set
	GetConfig    bool                // if true then config is returned as string
	EndpointType tether.EndpointType // only used when getConfig is true
}

type IDeviceBuilder interface {
	CreateDevice(dname string, configPath string, logLevel int) error          // creates and brings up a device
	StopDevice(dname string, configPath string) error                          // brings down a device
	ManageEndpoint(opts EndpointOptions) error                                 // add/remove/reset endpoint
	ManagePeer(dname string, opts PeerOptions) (string, error)                 // add/remove peer from device (and get config if requested)
	StartApi(dname string, apiURL string, errorCallback func(err error)) error // start API for device
	StopApi(dname string) error                                                // stop api of device
	StopClient()                                                               // closes tether client
}

func GetDeviceBuilder(builderType string, logger *logger.Logger) IDeviceBuilder {
	switch builderType {
	default:
		return &DefaultDBuilder{
			c:    tether.New(),
			l:    logger,
			apis: make(map[string]*api.Api),
		}
	}
}

// Device Director //

type DeviceDirector struct {
	Builder IDeviceBuilder
}

func NewDeviceDirector(db IDeviceBuilder) *DeviceDirector {
	return &DeviceDirector{
		Builder: db,
	}
}

func (d *DeviceDirector) SetBuilder(db IDeviceBuilder) {
	d.Builder = db
}

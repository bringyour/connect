package builder

import (
	"fmt"
	"net"

	"bringyour.com/connect/tether"
	"bringyour.com/tetherctl/api"
	"bringyour.com/tetherctl/helper"
	"bringyour.com/wireguard/conn"
	"bringyour.com/wireguard/device"
	"bringyour.com/wireguard/logger"
	"bringyour.com/wireguard/tun"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// Uses the ipv4 of the system as the only public ip of the device.
type DefaultDBuilder struct {
	l    *logger.Logger
	c    *tether.Client
	apis map[string]*api.Api
}

func (ddb *DefaultDBuilder) CreateDevice(dname string, configPath string, logLevel int) error {
	logger := logger.NewLogger(logLevel, fmt.Sprintf("(%s) ", dname))

	ip, err := helper.GetPublicIP(true)
	if err != nil {
		return fmt.Errorf("failed to get public IP: %w", err)
	}

	ipv4 := net.ParseIP(ip)
	if ipv4 == nil {
		return fmt.Errorf("failed to parse public IP: %w", err)
	}

	utun, err := tun.CreateUserspaceTUN(logger, &ipv4, nil)
	if err != nil {
		return fmt.Errorf("failed to create TUN device: %w", err)
	}
	ddb.l.Verbosef("TUN device created with ipv4:%s.", ipv4.String())

	wgDevice := device.NewDevice(utun, conn.NewDefaultBind(), logger)
	device := &tether.Device{Device: wgDevice, Addresses: []string{}}

	if err = ddb.c.AddDevice(dname, device); err != nil {
		return fmt.Errorf("error adding device: %w", err)
	}
	ddb.l.Verbosef("Device %q added successfully.", dname)

	finalConfPath := configPath + dname + ".conf"
	bywgConf, err := tether.ParseConfigFromFile(finalConfPath)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	ddb.l.Verbosef("Bringing up device %q from config file %q", dname, finalConfPath)

	if err = ddb.c.BringUpDevice(dname, bywgConf); err != nil {
		return fmt.Errorf("error bringing up device: %w", err)
	}
	ddb.l.Verbosef("Device %q brought up successfully.", dname)

	return nil
}

func (ddb *DefaultDBuilder) StopDevice(dname string, configPath string) error {
	finalConfPath := configPath + dname + ".conf"
	bywgConf, err := tether.ParseConfigFromFile(finalConfPath)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	ddb.l.Verbosef("Bringing down device %q from config file %q", dname, finalConfPath)

	if err := ddb.c.BringDownDevice(dname, bywgConf, finalConfPath); err != nil {
		return fmt.Errorf("error bringing down device: %w", err)
	}

	ddb.l.Verbosef("Device %q brought down successfully.", dname)
	return nil
}

func (ddb *DefaultDBuilder) StartApi(dname string, apiURL string, errorCallback func(err error)) error {
	if _, ok := ddb.apis[dname]; ok {
		return fmt.Errorf("API for device %q already running", dname)
	}
	apiOpts := api.ApiOptions{
		ApiUrl:       apiURL,
		Dname:        dname,
		TetherClient: ddb.c,
	}
	api, err := api.StartApi(apiOpts, errorCallback)
	if err != nil {
		return err
	}
	ddb.apis[dname] = api
	return nil
}

func (ddb *DefaultDBuilder) StopApi(dname string) error {
	if _, ok := ddb.apis[dname]; !ok {
		return nil
	}
	err := ddb.apis[dname].StopApi()
	ddb.apis[dname] = nil
	return err
}

func (ddb *DefaultDBuilder) StopClient() {
	for _, api := range ddb.apis {
		if api != nil {
			api.StopApi()
		}
	}
	ddb.apis = make(map[string]*api.Api)
	ddb.c.Close()
}

func (ddb *DefaultDBuilder) ManageEndpoint(opts EndpointOptions) error {
	if opts.Reset {
		ddb.c.ResetEndpoints()
		return nil
	}

	if opts.Remove {
		return ddb.c.RemoveEndpoint(opts.EndpointType)
	}

	return ddb.c.AddEndpoint(opts.EndpointType, opts.Endpoint)
}

func (ddb *DefaultDBuilder) ManagePeer(dname string, opts PeerOptions) (string, error) {
	pubKey, err := wgtypes.ParseKey(opts.PubKey)
	if err != nil {
		return "", fmt.Errorf("invalid peer key provided: %w", err)
	}

	if opts.Remove {
		return "", ddb.c.RemovePeerFromDevice(dname, pubKey)
	}

	if !opts.GetConfig {
		return "", ddb.c.AddPeerToDevice(dname, pubKey)
	}

	return ddb.c.AddPeerToDeviceAndGetConfig(dname, opts.PubKey, string(opts.EndpointType))
}

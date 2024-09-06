package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"bringyour.com/connect/tether"
	"bringyour.com/wireguard/conn"
	"bringyour.com/wireguard/device"
	"bringyour.com/wireguard/logger"
	"bringyour.com/wireguard/tun"
	"github.com/docopt/docopt-go"
)

type IDeviceBuilder interface {
	createDevice(c tether.Client, dname string, configPath string, logLevel int) error
	stopDevice(c tether.Client, configPath string) error
	startApi(apiURL string, errorCallback func(err error)) error
	stopApi()
}

func getDeviceBuilder(builderType string) IDeviceBuilder {
	switch builderType {
	case "test", "t":
		return &TestDBuilder{}
	default:
		return nil
	}
}

// Device Director //

type DeviceDirector struct {
	builder IDeviceBuilder
}

func newDeviceDirector(db IDeviceBuilder) *DeviceDirector {
	return &DeviceDirector{
		builder: db,
	}
}

// func (d *DeviceDirector) setBuilder(db IDeviceBuilder) {
// 	d.builder = db
// }

func build(opts docopt.Opts) {
	l.Verbosef("Device Builder.")

	var deviceBuilder IDeviceBuilder
	deviceTypes := []string{"test", "t"}
	for _, dt := range deviceTypes {
		if exists_, _ := opts.Bool("--" + dt); exists_ {
			deviceBuilder = getDeviceBuilder(dt)
			break
		}
	}
	if deviceBuilder == nil {
		l.Errorf("No device type specified")
		return
	}

	logL, _ := opts.String("--log")
	logLevel := getLogLevel(logL)

	deviceName := "tbywg0"
	configPath := "/root/connect/tetherctl/"
	apiURL := ":9090"

	director := newDeviceDirector(deviceBuilder)
	if err := director.builder.createDevice(tc, deviceName, configPath, logLevel); err != nil {
		l.Errorf("Error creating device: %v", err)
		return
	}

	// handle Ctrl+C for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	errorCallback := func(err error) {
		c <- syscall.SIGTERM
	}

	if err := director.builder.startApi(apiURL, errorCallback); err != nil {
		l.Errorf("Error starting API: %v", err)
		return
	}

	<-c
	l.Verbosef("Exiting...")
	director.builder.stopApi()
	if err := director.builder.stopDevice(tc, configPath); err != nil {
		l.Errorf("Error stopping device: %v", err)
	}
	tc.Close()
	l.Verbosef("Client closed successfully.")
}

// Test Device Builder //

type TestDBuilder struct {
	dname string
	api   *Api
}

func (tdb *TestDBuilder) createDevice(c tether.Client, dname string, configPath string, logLevel int) error {
	tdb.dname = dname
	logger := logger.NewLogger(logLevel, fmt.Sprintf("(%s) ", dname))

	ip, err := getPublicIP(true)
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
	l.Verbosef("TUN device created with ipv4:%s.", ipv4.String())

	wgDevice := device.NewDevice(utun, conn.NewDefaultBind(), logger)
	device := &tether.Device{Device: wgDevice, Addresses: []string{}}

	if err = c.AddDevice(dname, device); err != nil {
		return fmt.Errorf("error adding device: %w", err)
	}
	l.Verbosef("Device %q added successfully.", dname)

	finalConfPath := configPath + dname + ".conf"
	bywgConf, err := tether.ParseConfigFromFile(finalConfPath)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	l.Verbosef("Bringing up device %q from config file %q", dname, finalConfPath)

	if err = c.BringUpDevice(dname, bywgConf); err != nil {
		return fmt.Errorf("error bringing up device: %w", err)
	}
	l.Verbosef("Device %q brought up successfully.", dname)

	return nil
}

func (tdb *TestDBuilder) stopDevice(c tether.Client, configPath string) error {
	finalConfPath := configPath + tdb.dname + ".conf"
	bywgConf, err := tether.ParseConfigFromFile(finalConfPath)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	l.Verbosef("Bringing down device %q from config file %q", tdb.dname, finalConfPath)

	if err := c.BringDownDevice(tdb.dname, bywgConf, finalConfPath); err != nil {
		return fmt.Errorf("error bringing down device: %w", err)
	}

	l.Verbosef("Device %q brought down successfully.", tdb.dname)
	return nil
}

func (tdb *TestDBuilder) startApi(apiURL string, errorCallback func(err error)) error {
	apiOpts := ApiOptions{
		ApiUrl: apiURL,
		Dname:  tdb.dname,
	}
	api, err := startApi(apiOpts, errorCallback)
	tdb.api = api
	return err
}

func (tdb *TestDBuilder) stopApi() {
	if tdb.api == nil {
		return
	}
	tdb.api.stopApi()
	tdb.api = nil
}

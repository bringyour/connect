package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/docopt/docopt-go"

	"bringyour.com/connect/tether"
	"bringyour.com/tetherctl/builder"
	"bringyour.com/tetherctl/helper"
	"bringyour.com/wireguard/logger"
)

const TetherCtlVersion = "0.0.1"

var l = logger.NewLogger(logger.LogLevelVerbose, "(tetherctl) ") // global logger

func main() {
	usage := `Tether control.

Usage:
    tetherctl default-builder --dname=<dname> --config=<config> --api_url=<api_url> [--log=<log>]
    tetherctl cli
    
Options:
    -h --help               Show this screen.
    --version               Show version.
    --dname=<dname>         Wireguard device name.
    --config=<config>       Location of the config file in the system.
    --api_url=<api_url>     API url.
    --log=<log>             Log level from verbose(v), error(e) and silent(s) [default: error].
    `

	opts, err := docopt.ParseArgs(usage, os.Args[1:], TetherCtlVersion)
	if err != nil {
		panic(err)
	}

	if defaultBuilder_, _ := opts.Bool("default-builder"); defaultBuilder_ {
		defaultBuilder(opts)
	} else if cli_, _ := opts.Bool("cli"); cli_ {
		cli()
	} else {
		docopt.PrintHelpAndExit(nil, usage)
	}
}

func defaultBuilder(opts docopt.Opts) {
	dbl := logger.NewLogger(logger.LogLevelVerbose, "(builder) ")
	deviceBuilder := builder.GetDeviceBuilder("", dbl)

	deviceName, err := opts.String("--dname")
	if err != nil || deviceName == "" {
		dbl.Errorf("No device name provided")
		return
	}

	configPath, err := opts.String("--config")
	if err != nil || configPath == "" {
		dbl.Errorf("No config path provided")
		return
	}

	apiURL, err := opts.String("--api_url")
	if err != nil || apiURL == "" {
		dbl.Errorf("No API URL provided")
		return
	}

	logL, _ := opts.String("--log")
	logLevel := helper.GetLogLevel(logL)

	director := builder.NewDeviceDirector(deviceBuilder)

	// add ipv4 endpoint for peer
	ipv4, err := helper.GetPublicIP(true)
	if err != nil {
		dbl.Errorf("Failed to get peer endpoint")
		return
	}
	endpOpts := builder.EndpointOptions{
		EndpointType: tether.EndpointIPv4,
		Endpoint:     ipv4,
	}
	if err := director.Builder.ManageEndpoint(endpOpts); err != nil {
		panic(err)
	}

	// create and start device
	if err := director.Builder.CreateDevice(deviceName, configPath, logLevel); err != nil {
		dbl.Errorf("Error creating device: %v", err)
		return
	}

	// handle Ctrl+C for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// start api
	errorCallback := func(err error) {
		dbl.Errorf("Error running API: %v", err)
		c <- syscall.SIGTERM
	}
	if err := director.Builder.StartApi(deviceName, apiURL, errorCallback); err != nil {
		dbl.Errorf("Error starting API: %v", err)
		c <- syscall.SIGTERM
	}

	// close/stop everything
	<-c
	dbl.Verbosef("Exiting...")
	if err := director.Builder.StopApi(deviceName); err != nil {
		dbl.Errorf("Error stopping API: %v", err)
	}
	if err := director.Builder.StopDevice(deviceName, configPath); err != nil {
		dbl.Errorf("Error stopping device: %v", err)
	}
	director.Builder.StopClient()
	dbl.Verbosef("Client stoped successfully.")
}

package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"bringyour.com/tetherctl/api"
	"bringyour.com/tetherctl/helper"
	"bringyour.com/wireguard/conn"
	"bringyour.com/wireguard/device"
	"github.com/docopt/docopt-go"
	"github.com/mattn/go-shellwords"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	"bringyour.com/connect/tether"
	"bringyour.com/wireguard/logger"
	"bringyour.com/wireguard/tun"
)

const DefaultDeviceName = "bywg0"
const DefaultConfigFile = "/etc/tethercli/"
const DefaultApiUrl = "localhost:9090"
const DefaultEndpointType = string(tether.EndpointIPv4)

var tc tether.Client

var cliDeviceName string = DefaultDeviceName
var apiServer *api.Api

func cli() {
	usage := fmt.Sprintf(
		`Tether cli.

Usage:
    tethercli add [--dname=<dname>] [--log=<log>] [--ipv4=<ipv4>] [--ipv6=<ipv6>]
    tethercli remove [--dname=<dname>]
    tethercli up [--dname=<dname>] [--config=<config>]
    tethercli down [--dname=<dname>] [--config=<config>] [--new_file=<new_file>]
    tethercli get-config [--dname=<dname>] [--config=<config>]
    tethercli save-config [--dname=<dname>] [--config=<config>] [--new_file=<new_file>]
    tethercli gen-priv-key 
    tethercli gen-pub-key --priv_key=<priv_key>
    tethercli get-device-names
    tethercli get-device [--dname=<dname>]
    tethercli change-device [--dname=<dname>] [--lport=<lport>]	[--priv_key=<priv_key>]
    tethercli add-peer --pub_key=<pub_key> [--dname=<dname>] [--endpoint_type=<endpoint_type>]
    tethercli remove-peer --pub_key=<pub_key> [--dname=<dname>]
    tethercli get-peer-config --pub_key=<pub_key> [--dname=<dname>] [--endpoint_type=<endpoint_type>]
    tethercli start-api [--dname=<dname>] [--api_url=<api_url>]
    tethercli stop-api
    tethercli test
    
Options:
    -h --help                         Show this screen.
    --version                         Show version.
    --dname=<dname>                   Wireguard device name. Keeps the last set value [initial: %s].
    --config=<config>                 Location of the config file in the system [default: %s].
    --log=<log>                       Log level from verbose, error and silent [default: error].
    --ipv4=<ipv4>                     Public IPv4 address of the device.
    --ipv6=<ipv6>                     Public IPv6 address of the device.
    --new_file=<new_file>             Location where the updated config should be stored. If not specified the original file is updated.
    --endpoint_type=<endpoint_type>   Type of endpoint to use for peer config [default: %s].
    --lport=<lport>                   Port to listen on for incoming connections.
    --pub_key=<pub_key>               Public key of a WireGuard peer (unique).
    --priv_key=<priv_key>             Private key of a WireGuard device.
    --api_url=<api_url>               API url [default: %s].

    `,
		DefaultDeviceName,
		DefaultConfigFile,
		DefaultEndpointType,
		DefaultApiUrl,
	)

	tc = *tether.New() // create client

	// add ipv4 endpoint for peer
	ipv4, err := helper.GetPublicIP(true)
	if err != nil {
		l.Errorf("Failed to get peer endpoint")
		return
	}
	if err := tc.AddEndpoint(tether.EndpointIPv4, ipv4); err != nil {
		panic(err)
	}

	l.Verbosef("Tether cli started.")

	// handle Ctrl+C for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		l.Verbosef("Exiting...")
		stopApiCli()
		tc.Close()
		os.Exit(0)
	}()

	// handle help message
	docopt.DefaultParser.HelpHandler = func(err error, usage string) {
		if err == nil {
			fmt.Println(usage)
		} else {
			l.Errorf("Invalid command or arguments. Use 'tethercli --help' for usage.")
		}
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ") // prompt

		// read a line of input from the user
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		// parse the input into arguments (enables qoutation marks)
		args, err := shellwords.Parse(input)
		if err != nil {
			docopt.DefaultParser.HelpHandler(err, usage)
			continue // continue to the next iteration on error
		}

		opts, err := docopt.ParseArgs(usage, args, TetherCtlVersion)
		if err != nil {
			continue // continue to the next iteration on error
		}

		// only change device name (dname arg) if it was provided
		// otherwire dname stores oldest value (initially DefaultDeviceName)
		_deviceName, err := opts.String("--dname")
		if err == nil {
			cliDeviceName = _deviceName
		}

		if add_, _ := opts.Bool("add"); add_ {
			addCli(opts)
		} else if remove_, _ := opts.Bool("remove"); remove_ {
			removeCli()
		} else if up_, _ := opts.Bool("up"); up_ {
			upCli(opts)
		} else if down_, _ := opts.Bool("down"); down_ {
			downCli(opts)
		} else if getConfig_, _ := opts.Bool("get-config"); getConfig_ {
			getConfigCli(opts)
		} else if saveConfig_, _ := opts.Bool("save-config"); saveConfig_ {
			saveConfigCli(opts)
		} else if genPrivKey_, _ := opts.Bool("gen-priv-key"); genPrivKey_ {
			genPrivKeyCli()
		} else if genPubKey_, _ := opts.Bool("gen-pub-key"); genPubKey_ {
			genPubKeyCli(opts)
		} else if getDeviceNames_, _ := opts.Bool("get-device-names"); getDeviceNames_ {
			getDeviceNamesCli()
		} else if getDevice_, _ := opts.Bool("get-device"); getDevice_ {
			getDeviceCli()
		} else if setupDevice_, _ := opts.Bool("change-device"); setupDevice_ {
			changeDeviceCli(opts)
		} else if addPeer_, _ := opts.Bool("add-peer"); addPeer_ {
			addPeerCli(opts)
		} else if removePeer_, _ := opts.Bool("remove-peer"); removePeer_ {
			removePeerCli(opts)
		} else if getPeerConfig_, _ := opts.Bool("get-peer-config"); getPeerConfig_ {
			getPeerConfigCli(opts)
		} else if startAPI_, _ := opts.Bool("start-api"); startAPI_ {
			startApiCli(opts)
		} else if stopAPI_, _ := opts.Bool("stop-api"); stopAPI_ {
			stopApiCli()
		}
	}
}

func addCli(opts docopt.Opts) {

	// logger

	logL, _ := opts.String("--log")
	logger := logger.NewLogger(helper.GetLogLevel(logL), fmt.Sprintf("(%s) ", cliDeviceName))

	// ipv4 and ipv6

	pubIPv4, err1 := opts.String("--ipv4")
	var pubIPv4P *net.IP // nil if pubIPv4 is not provided and default is not possible
	if err1 != nil {
		// no ipv4 provided try default
		ip, err2 := helper.GetPublicIP(true)
		if err2 != nil {
			pubIPv4 = ""
		} else {
			pubIPv4 = ip
		}
	}
	if pubIPv4 != "" {
		parsedIP := net.ParseIP(pubIPv4)
		if parsedIP == nil {
			l.Errorf("Invalid IPv4 address provided: %v", pubIPv4)
			return
		}
		pubIPv4P = &parsedIP
	}

	pubIPv6, err1 := opts.String("--ipv6")
	var pubIPv6P *net.IP // nil if pubIPv6 is not provided and default is not possible
	if err1 != nil {
		// no ipv6 provided try default
		ip, err2 := helper.GetPublicIP(false)
		if err2 != nil {
			pubIPv6 = ""
		} else {
			pubIPv6 = ip
		}
	}
	if pubIPv6 != "" {
		parsedIP := net.ParseIP(pubIPv6)
		if parsedIP == nil {
			l.Errorf("Invalid IPv6 address provided: %v", pubIPv6)
			return
		}
		pubIPv6P = &parsedIP
	}

	if pubIPv4P == nil && pubIPv6P == nil {
		l.Errorf("No public IPv4 or IPv6 provided and could not get the public IP of this machine.")
		return
	}

	// create tun

	utun, err := tun.CreateUserspaceTUN(logger, pubIPv4P, pubIPv6P)
	if err != nil {
		logger.Errorf("Failed to create TUN device: %v", err)
		os.Exit(1)
	}

	ipv4Str, ipv6Str := "none", "none"
	if pubIPv4P != nil {
		ipv4Str = pubIPv4P.String()
	}
	if pubIPv6P != nil {
		ipv6Str = pubIPv6P.String()
	}
	l.Verbosef("TUN device created with ipv4:%s and ipv6:%s.", ipv4Str, ipv6Str)

	// create device

	wgDevice := device.NewDevice(utun, conn.NewDefaultBind(), logger)
	logger.Verbosef("Device started")
	device := &tether.Device{Device: wgDevice, Addresses: []string{}}

	// add device

	err = tc.AddDevice(cliDeviceName, device)
	if err != nil {
		l.Errorf("Error adding device: %v", err)
		return
	}

	l.Verbosef("Device %q added successfully.", cliDeviceName)
}

func removeCli() {
	err := tc.RemoveDevice(cliDeviceName)
	if err != nil {
		l.Errorf("Error removing device: %v", err)
		return
	}
	l.Verbosef("Device %q removed successfully.", cliDeviceName)
}

func upCli(opts docopt.Opts) {
	configFile, err := opts.String("--config")
	if err != nil {
		configFile = DefaultConfigFile
	}
	finalConfPath := configFile + cliDeviceName + ".conf"

	l.Verbosef("Bringing up device %q from config file %q", cliDeviceName, finalConfPath)

	bywgConf, err := tether.ParseConfigFromFile(finalConfPath)
	if err != nil {
		l.Errorf("Error parsing config: %v", err)
		return
	}

	err = tc.BringUpDevice(cliDeviceName, bywgConf)
	if err != nil {
		l.Errorf("Error bringing up device: %v", err)
		return
	}

	l.Verbosef("Device %q brought up successfully.", cliDeviceName)
}

func downCli(opts docopt.Opts) {
	configFile, err := opts.String("--config")
	if err != nil {
		configFile = DefaultConfigFile
	}
	finalConfPath := configFile + cliDeviceName + ".conf"

	newFile, err := opts.String("--new_file")
	if err != nil {
		newFile = configFile
	}
	finalNewPath := newFile + cliDeviceName + ".conf"

	l.Verbosef("Bringing down device %q from config file %q", cliDeviceName, finalConfPath)

	bywgConf, err := tether.ParseConfigFromFile(finalConfPath)
	if err != nil {
		l.Errorf("Error parsing config: %v", err)
		return
	}

	err = tc.BringDownDevice(cliDeviceName, bywgConf, finalNewPath)
	if err != nil {
		l.Errorf("Error bringing down device: %v", err)
		return
	}

	l.Verbosef("Device %q brought down successfully.", cliDeviceName)
}

func getConfigCli(opts docopt.Opts) {
	configFile, err := opts.String("--config")
	if err != nil {
		configFile = DefaultConfigFile
	}
	finalConfPath := configFile + cliDeviceName + ".conf"

	l.Verbosef("Getting updated config for file %q of device %q", finalConfPath, cliDeviceName)

	bywgConf, err := tether.ParseConfigFromFile(finalConfPath)
	if err != nil {
		l.Errorf("Error parsing config: %v", err)
		return
	}

	config, err := tc.GetUpdatedConfig(cliDeviceName, bywgConf)
	if err != nil {
		l.Errorf("Error getting updated config: %v", err)
		return
	}
	fmt.Println(config)
}

func saveConfigCli(opts docopt.Opts) {
	configFile, err := opts.String("--config")
	if err != nil {
		configFile = DefaultConfigFile
	}
	finalConfPath := configFile + cliDeviceName + ".conf"

	newFile, err := opts.String("--new_file")
	if err != nil {
		newFile = configFile
	}
	finalNewPath := newFile + cliDeviceName + ".conf"

	l.Verbosef("Saving config based on config file %q and device %q to %q", finalConfPath, cliDeviceName, finalNewPath)

	bywgConf, err := tether.ParseConfigFromFile(finalConfPath)
	if err != nil {
		l.Errorf("Error parsing config: %v", err)
		return
	}

	err = tc.SaveConfigToFile(cliDeviceName, bywgConf, finalNewPath)
	if err != nil {
		l.Errorf("Error saving config to file: %v", err)
		return
	}

	l.Verbosef("Config file %q for device %q saved successfully.", finalNewPath, cliDeviceName)
}

func genPrivKeyCli() {
	privateKeyGenerated, err := wgtypes.GeneratePrivateKey()
	if err != nil {
		l.Errorf("failed to generate private key: %w", err)
		return
	}
	fmt.Println(privateKeyGenerated.String())
}

func genPubKeyCli(opts docopt.Opts) {
	privateKey, err := opts.String("--priv_key")
	if err != nil {
		l.Errorf("Error: no --priv_key provided: %v", err)
		return
	}

	_pk, err := wgtypes.ParseKey(privateKey)
	if err != nil {
		l.Errorf("Invalid private key provided: %w", err)
		return
	}

	fmt.Println(_pk.PublicKey())
}

func getDeviceNamesCli() {
	deviceNames := tc.GetAvailableDevices()
	if len(deviceNames) == 0 {
		l.Verbosef("No devices found.")
		return
	}
	l.Verbosef("Found %d device(s) - '%s'", len(deviceNames), strings.Join(deviceNames, "', '"))
}

func getDeviceCli() {
	config, err := tc.GetDeviceFormatted(cliDeviceName)
	if err != nil {
		l.Errorf("Error getting device information: %v", err)
		return
	}
	fmt.Println(config)
}

func changeDeviceCli(opts docopt.Opts) {
	listenPort, err := opts.Int("--lport")
	var listenPortP *int // if listenPort is not provided then listenPortP is nil
	if err == nil {
		listenPortP = &listenPort
	}

	privateKey, err := opts.String("--priv_key")
	var privKeyP *wgtypes.Key // if privateKey is not provided then privKeyP is nil
	if err == nil {
		privKey, err := wgtypes.ParseKey(privateKey)
		if err != nil {
			l.Errorf("Invalid private key provided. Change not possible.")
			return
		}
		privKeyP = &privKey
	}

	if privKeyP == nil && listenPortP == nil {
		l.Verbosef("Warning! No changes requested to device %q.", cliDeviceName)
	} else {
		//  at least one of the fields is not nil
		if err := tc.ConfigureDevice(cliDeviceName, wgtypes.Config{
			PrivateKey: privKeyP,
			ListenPort: listenPortP,
		}); err != nil {
			l.Errorf("Error configuring device: %v", err)
			return
		}
		l.Verbosef("Device %q changed successfully.", cliDeviceName)
	}
}

func addPeerCli(opts docopt.Opts) {
	publicKey, err := opts.String("--pub_key")
	if err != nil {
		l.Errorf("Error: no --pub_key provided: %v", err)
		return
	}

	endpointType, err1 := opts.String("--endpoint_type")
	if err1 != nil {
		endpointType = DefaultEndpointType
	}

	config, err := tc.AddPeerToDeviceAndGetConfig(cliDeviceName, publicKey, endpointType)
	if err != nil {
		l.Errorf("Error adding peer to device: %v", err)
		return
	}

	fmt.Println(config)
}

func removePeerCli(opts docopt.Opts) {
	publicKey, err := opts.String("--pub_key")
	if err != nil {
		l.Errorf("Error: no --pub_key provided: %v", err)
		return
	}

	_pk, err := wgtypes.ParseKey(publicKey)
	if err != nil {
		l.Errorf("Error parsing public key: %v", err)
		return
	}

	err = tc.RemovePeerFromDevice(cliDeviceName, _pk)
	if err != nil {
		l.Errorf("Error removing peer from device: %v", err)
		return
	}

	l.Verbosef("Peer %s removed successfully.", publicKey)
}

func getPeerConfigCli(opts docopt.Opts) {
	publicKey, err := opts.String("--pub_key")
	if err != nil {
		l.Errorf("Error: no --pub_key provided: %v", err)
		return
	}

	endpointType, err1 := opts.String("--endpoint_type")
	if err1 != nil {
		endpointType = DefaultEndpointType
	}

	config, err := tc.GetPeerConfig(cliDeviceName, publicKey, endpointType)
	if err != nil {
		l.Errorf("Error getting peer config: %v", err)
		return
	}

	fmt.Println(config)
}

func startApiCli(opts docopt.Opts) {
	if apiServer != nil {
		l.Errorf("API server already running")
		return
	}

	apiUrl, err := opts.String("--api_url")
	if err != nil {
		apiUrl = DefaultApiUrl
	}

	apiOpts := api.ApiOptions{
		ApiUrl:       apiUrl,
		Dname:        cliDeviceName,
		TetherClient: &tc,
	}

	errorCallback := func(e error) {
		l.Errorf("Error running API: %v", e)
		stopApiCli()
	}

	apiServer, err = api.StartApi(apiOpts, errorCallback)
	if err != nil {
		l.Errorf("Error starting API: %v", err)
	} else {
		l.Verbosef("Started API server at %s for device %q", apiUrl, cliDeviceName)
	}
}

func stopApiCli() {
	if apiServer == nil {
		l.Verbosef("API server not running")
		return
	}
	if err := apiServer.StopApi(); err != nil {
		l.Errorf("Error stopping API: %v", err)
	} else {
		l.Verbosef("API server stopped.")
	}
	apiServer = nil
}

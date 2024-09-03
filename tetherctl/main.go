package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"bringyour.com/wireguard/conn"
	"bringyour.com/wireguard/device"
	"github.com/docopt/docopt-go"
	"github.com/gin-gonic/gin"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	"bringyour.com/connect/tether"
	"bringyour.com/wireguard/logger"
	"bringyour.com/wireguard/tun"
)

const TetherCtlVersion = "0.0.1"

const DefaultDeviceName = "bywg0"
const DefaultConfigFile = "/etc/tetherctl/"
const DefaultApiUrl = "localhost:9090"

var tc tether.Client
var dName string = DefaultDeviceName
var apiServer *http.Server

var l = logger.NewLogger(logger.LogLevelVerbose, "(tetherctl) ") // global logger

func main() {
	usage := fmt.Sprintf(
		`Tether control.

Usage:
    tetherctl add [--dname=<dname>] [--log=<log>] [--ipv4=<ipv4>] [--ipv6=<ipv6>]
    tetherctl remove [--dname=<dname>]
    tetherctl up [--dname=<dname>] [--config=<config>]
    tetherctl down [--dname=<dname>] [--config=<config>] [--new_file=<new_file>]
    tetherctl get-config [--dname=<dname>] [--config=<config>]
    tetherctl save-config [--dname=<dname>] [--config=<config>] [--new_file=<new_file>]
    tetherctl gen-priv-key 
    tetherctl gen-pub-key --priv_key=<priv_key>
    tetherctl get-device-names
    tetherctl get-device [--dname=<dname>]
    tetherctl change-device [--dname=<dname>] [--lport=<lport>]	[--priv_key=<priv_key>]
    tetherctl add-peer --pub_key=<pub_key> [--dname=<dname>] [--endpoint=<endpoint>]
    tetherctl remove-peer --pub_key=<pub_key> [--dname=<dname>]
    tetherctl get-peer-config --pub_key=<pub_key> [--dname=<dname>] [--endpoint=<endpoint>]
    tetherctl start-api [--dname=<dname>] [--endpoint=<endpoint>] [--api_url=<api_url>]
    tetherctl stop-api
    tetherctl test
    
Options:
    -h --help               Show this screen.
    --version               Show version.
    --dname=<dname>         Wireguard device name. Keeps the last set value [initial: %q].
    --config=<config>       Location of the config file in the system [default: %q].
    --log=<log>             Log level from verbose, error and silent [default: error].
    --ipv4=<ipv4>           Public IPv4 address of the device.
    --ipv6=<ipv6>           Public IPv6 address of the device.
    --new_file=<new_file>   Location where the updated config should be stored. If not specified the original file is updated.
    --endpoint=<endpoint>   Wireguard url/ip where server can be found [default: this_pc_public_ip].
    --lport=<lport>         Port to listen on for incoming connections.
    --pub_key=<pub_key>     Public key of a WireGuard peer (unique).
    --priv_key=<priv_key>   Private key of a WireGuard device.
    --api_url=<api_url>     API url [default: %q].

    `,
		DefaultDeviceName,
		DefaultConfigFile,
		DefaultApiUrl,
	)

	tc = *tether.New() // create client

	// handle Ctrl+C for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		l.Verbosef("Exiting...")
		tc.Close()
		stopApiCli()
		os.Exit(0)
	}()

	// handle help message
	docopt.DefaultParser.HelpHandler = func(err error, usage string) {
		if err == nil {
			fmt.Println(usage)
		} else {
			l.Errorf("Invalid command or arguments. Use 'tetherctl --help' for usage.")
		}
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ") // prompt

		// read a line of input from the user
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		opts, err := docopt.ParseArgs(usage, strings.Fields(input), TetherCtlVersion)
		if err != nil {
			continue // continue to the next iteration on error
		}

		// only change device name (dname arg) if it was provided
		// otherwire dname stores oldest value (initially DefaultDeviceName)
		_deviceName, err := opts.String("--dname")
		if err == nil {
			dName = _deviceName
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
		} else if test_, _ := opts.Bool("test"); test_ {
			Test()
		}
	}
}

func Test() {
	l.Verbosef("Test")
}

// get ip{v4,v6} of this machine
func getPublicIP(isIPv4 bool) (string, error) {
	request := "https://api.ipify.org?format=text"
	if !isIPv4 {
		request = "https://api6.ipify.org?format=text"
	}
	resp, err := http.Get(request)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	ip, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(ip), nil
}

func addCli(opts docopt.Opts) {

	// logger

	logL, _ := opts.String("--log")
	logL = strings.ToLower(logL)
	logLevel := logger.LogLevelError // default to error logging
	switch logL {
	case "verbose", "debug", "v", "d":
		logLevel = logger.LogLevelVerbose
	case "silent", "s":
		logLevel = logger.LogLevelSilent
	}
	logger := logger.NewLogger(logLevel, fmt.Sprintf("(%s) ", dName))

	// ipv4 and ipv6

	pubIPv4, err1 := opts.String("--ipv4")
	var pubIPv4P *net.IP // nil if pubIPv4 is not provided and default is not possible
	if err1 != nil {
		// no ipv4 provided try default
		ip, err2 := getPublicIP(true)
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
		ip, err2 := getPublicIP(false)
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
	l.Verbosef("TUN device created with ipv4:%s and ipv6:%s.\n", ipv4Str, ipv6Str)

	// create device

	wgDevice := device.NewDevice(utun, conn.NewDefaultBind(), logger)
	logger.Verbosef("Device started")
	device := &tether.Device{Device: wgDevice, Addresses: []string{}}

	// add device

	err = tc.AddDevice(dName, device)
	if err != nil {
		l.Errorf("Error adding device: %v", err)
		return
	}

	l.Verbosef("Device %q added successfully.\n", dName)
}

func removeCli() {
	err := tc.RemoveDevice(dName)
	if err != nil {
		l.Errorf("Error removing device: %v", err)
		return
	}
	l.Verbosef("Device %q removed successfully.\n", dName)
}

func upCli(opts docopt.Opts) {
	configFile, err := opts.String("--config")
	if err != nil {
		configFile = DefaultConfigFile
	}
	finalConfPath := configFile + dName + ".conf"

	l.Verbosef("Bringing up interface %q from config file %q\n", dName, finalConfPath)

	bywgConf, err := tether.ParseConfig(finalConfPath)
	if err != nil {
		l.Errorf("Error parsing config: %v", err)
		return
	}

	err = tc.BringUpDevice(dName, bywgConf)
	if err != nil {
		l.Errorf("Error bringing up interface: %v", err)
		return
	}

	l.Verbosef("Interface %q brought up successfully.\n", dName)
}

func downCli(opts docopt.Opts) {
	configFile, err := opts.String("--config")
	if err != nil {
		configFile = DefaultConfigFile
	}
	finalConfPath := configFile + dName + ".conf"

	newFile, err := opts.String("--new_file")
	if err != nil {
		newFile = configFile
	}
	finalNewPath := newFile + dName + ".conf"

	l.Verbosef("Bringing down interface %q from config file %q\n", dName, finalConfPath)

	bywgConf, err := tether.ParseConfig(finalConfPath)
	if err != nil {
		l.Errorf("Error parsing config: %v", err)
		return
	}

	err = tc.BringDownDevice(dName, bywgConf, finalNewPath)
	if err != nil {
		l.Errorf("Error bringing down interface: %v", err)
		return
	}

	l.Verbosef("Interface %q brought down successfully.\n", dName)
}

func getConfigCli(opts docopt.Opts) {
	configFile, err := opts.String("--config")
	if err != nil {
		configFile = DefaultConfigFile
	}
	finalConfPath := configFile + dName + ".conf"

	l.Verbosef("Getting updated config for file %q of device %q\n", finalConfPath, dName)

	bywgConf, err := tether.ParseConfig(finalConfPath)
	if err != nil {
		l.Errorf("Error parsing config: %v", err)
		return
	}

	config, err := tc.GetUpdatedConfig(dName, bywgConf)
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
	finalConfPath := configFile + dName + ".conf"

	newFile, err := opts.String("--new_file")
	if err != nil {
		newFile = configFile
	}
	finalNewPath := newFile + dName + ".conf"

	l.Verbosef("Saving config based on config file %q and device %q to %q\n", finalConfPath, dName, finalNewPath)

	bywgConf, err := tether.ParseConfig(finalConfPath)
	if err != nil {
		l.Errorf("Error parsing config: %v", err)
		return
	}

	err = tc.SaveConfigToFile(dName, bywgConf, finalNewPath)
	if err != nil {
		l.Errorf("Error saving config to file: %v", err)
		return
	}

	l.Verbosef("Config file %q for device %q saved successfully.\n", finalNewPath, dName)
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
	l.Verbosef("Found %d device(s) - %s\n", len(deviceNames), strings.Join(deviceNames, ", "))
}

func getDeviceCli() {
	config, err := tc.GetDeviceFormatted(dName)
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
		l.Verbosef("Warning! No changes requested to device %s.\n", dName)
	} else {
		//  at least one of the fields is not nil
		if err := tc.ConfigureDevice(dName, wgtypes.Config{
			PrivateKey: privKeyP,
			ListenPort: listenPortP,
		}); err != nil {
			l.Errorf("Error configuring device: %v", err)
			return
		}
		l.Verbosef("Device %q changed successfully.\n", dName)
	}
}

func addPeerCli(opts docopt.Opts) {
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

	err = tc.AddPeerToDevice(dName, _pk)
	if err != nil {
		l.Errorf("Error adding peer to device: %v", err)
		return
	}

	getPeerConfigCli(opts)
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

	err = tc.RemovePeerFromDevice(dName, _pk)
	if err != nil {
		l.Errorf("Error removing peer from device: %v", err)
		return
	}

	l.Verbosef("Peer %s removed successfully.\n", publicKey)
}

func getPeerConfigCli(opts docopt.Opts) {
	publicKey, err := opts.String("--pub_key")
	if err != nil {
		l.Errorf("Error: no --pub_key provided: %v", err)
		return
	}

	wgEndpoint, err1 := opts.String("--endpoint")
	if err1 != nil {
		ipv4, err2 := getPublicIP(true)
		if err2 != nil {
			ipv6, err3 := getPublicIP(false)
			if err3 != nil {
				l.Errorf("Could not get the public IP of this machine: %v", err2)
				return
			} else {
				wgEndpoint = ipv6
			}
		} else {
			wgEndpoint = ipv4
		}
	}

	config, err := tc.GetPeerConfig(dName, publicKey, wgEndpoint)
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

	wgEndpoint, err1 := opts.String("--endpoint")
	if err1 != nil {
		ipv4, err2 := getPublicIP(true)
		if err2 != nil {
			ipv6, err3 := getPublicIP(false)
			if err3 != nil {
				l.Errorf("Could not get the public IP of this machine: %v", err2)
				return
			} else {
				wgEndpoint = ipv6
			}
		} else {
			wgEndpoint = ipv4
		}
	}

	router := gin.Default()
	// endpoint for adding peer and returning config
	router.POST("/peer/add/*pubkey", func(c *gin.Context) { apiAddPeer(c, wgEndpoint) })
	// endpoint for removing peer
	router.DELETE("/peer/remove/*pubkey", func(c *gin.Context) { apiRemovePeer(c) })
	// endpoint for getting existing peer config
	router.GET("/peer/config/*pubkey", func(c *gin.Context) { apiGetPeerConfig(c, wgEndpoint) })

	// wrap Gin router in an HTTP server
	srv := &http.Server{
		Addr:    apiUrl,
		Handler: router,
	}

	l.Verbosef("Starting API server at %s\n", apiUrl)

	// start server in goroutine and wait for interrupt signal
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			l.Errorf("Error listen: %s\n", err)
			return
		}
	}()

	apiServer = srv
}

func stopApiCli() {
	if apiServer == nil {
		return
	}
	// try to shutdown the server gracefully (wait max 5 secs to finish pending requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := apiServer.Shutdown(ctx); err != nil {
		l.Errorf("Server forced to shutdown: %v", err)
		return
	}
	l.Verbosef("API Server stopped")
	apiServer = nil
}

func apiGetPeerConfig(context *gin.Context, wgEndpoint string) {
	publicKey := parsePublicKey(context)

	config, err := tc.GetPeerConfig(dName, publicKey, wgEndpoint)
	if err != nil {
		context.String(http.StatusNotFound, fmt.Sprintf("%d Not Found - %v", http.StatusNotFound, err))
		return
	}

	// return the config as plaintext in the response body
	context.String(http.StatusOK, config)
}

func apiAddPeer(context *gin.Context, wgEndpoint string) {
	publicKey := parsePublicKey(context)

	_pk, err := wgtypes.ParseKey(publicKey)
	if err != nil {
		context.String(http.StatusBadRequest, fmt.Sprintf("%d Bad Request - Invalid public key %q", http.StatusBadRequest, publicKey))
		return
	}

	err = tc.AddPeerToDevice(dName, _pk)
	if err != nil {
		context.String(http.StatusInternalServerError, fmt.Sprintf("%d Internal Server Error - %v", http.StatusInternalServerError, err))
		return
	}

	config, err := tc.GetPeerConfig(dName, publicKey, wgEndpoint)
	if err != nil {
		context.String(http.StatusNotFound, fmt.Sprintf("%d Not Found - %v", http.StatusNotFound, err))
		return
	}

	// return the config as plaintext in the response body
	context.String(http.StatusOK, config)
}

func apiRemovePeer(context *gin.Context) {
	publicKey := parsePublicKey(context)

	_pk, err := wgtypes.ParseKey(publicKey)
	if err != nil {
		context.String(http.StatusBadRequest, fmt.Sprintf("%d Bad Request - Invalid public key %q", http.StatusBadRequest, publicKey))
		return
	}

	err = tc.RemovePeerFromDevice(dName, _pk)
	if err != nil {
		context.String(http.StatusInternalServerError, fmt.Sprintf("%d Internal Server Error - %v", http.StatusInternalServerError, err))
		return
	}

	context.String(http.StatusOK, fmt.Sprintf("%d OK - Peer %q removed successfully", http.StatusOK, publicKey))
}

func parsePublicKey(context *gin.Context) string {
	publicKey := context.Param("pubkey")

	// remove leading slash since we are using wildcards (should always be trimmed)
	publicKey = strings.TrimPrefix(publicKey, "/")

	return publicKey
}

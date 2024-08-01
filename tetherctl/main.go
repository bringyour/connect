package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/docopt/docopt-go"
	"github.com/gin-gonic/gin"
	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	"bringyour.com/connect/tether"
)

const TetherCtlVersion = "0.0.1"

const DefaultDeviceName = "bywg0"
const DefaultConfigFile = "/etc/tetherctl/"
const DefaultApiUrl = "localhost:9090"

var tc tether.TetherClient

func main() {
	usage := fmt.Sprintf(
		`Tether control.

Usage:
	tetherctl up [--device_name=<device_name>]
		[--config_file=<config_file>]
	tetherctl down [--device_name=<device_name>]
		[--config_file=<config_file>]
	tetherctl gen-priv-key
	tetherctl gen-pub-key --priv_key=<priv_key>
	tetherctl get-device-names
	tetherctl get-device [--device_name=<device_name>]
	tetherctl change-device [--device_name=<device_name>] 
		[--listen_port=<listen_port>]
		[--priv_key=<priv_key>]
	tetherctl add-peer [--device_name=<device_name>] [--endpoint=<endpoint>]
		--pub_key=<pub_key> 
	tetherctl get-peer-config [--device_name=<device_name>] [--endpoint=<endpoint>]
		--pub_key=<pub_key>
    tetherctl start-api [--device_name=<device_name>] [--endpoint=<endpoint>]
		[--api_url=<api_url>]
	tetherctl hello [--device_name=<device_name>]
	tetherctl test [--device_name=<device_name>]
    
Options:
    -h --help                       Show this screen.
    --version                       Show version.
    --device_name=<device_name> 	Wireguard device name [default: %q].
	--endpoint=<endpoint> 			Wireguard url/ip where server can be found [default: this_pc_public_ip].
	--config_file					Location of the config file in the system [default: %q]
	--pub_key=<pub_key> 			Public key of a peer (unique)
	--listen_port=<listen_port> 	Port to listen on for incoming connections.
	--priv_key=<priv_key> 			Private key of the device.
    --api_url=<api_url> 			API url [default: %q]

    `,
		DefaultDeviceName,
		DefaultConfigFile,
		DefaultApiUrl,
	)

	opts, err := docopt.ParseArgs(usage, os.Args[1:], TetherCtlVersion)
	if err != nil {
		panic(err)
	}

	deviceName, err := opts.String("--device_name")
	if err != nil {
		deviceName = DefaultDeviceName
	}

	wgEndpoint, err := opts.String("--endpoint")
	if err != nil {
		wgEndpoint = getPublicIP()
	}

	c, err := wgctrl.New()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	tc = tether.TetherClient{
		Client:     c,
		ClientIP:   net.ParseIP(wgEndpoint),
		DeviceName: deviceName,
	}
	// fmt.Printf("\033[34mTether client created for IP %q and device %q\033[0m\n\n", wgEndpoint, deviceName)

	if up_, _ := opts.Bool("up"); up_ {
		upCli(opts)
	} else if down_, _ := opts.Bool("down"); down_ {
		downCli(opts)
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
	} else if getPeerConfig_, _ := opts.Bool("get-peer-config"); getPeerConfig_ {
		getPeerConfigCli(opts)
	} else if startAPI_, _ := opts.Bool("start-api"); startAPI_ {
		startApiCli(opts)
	} else if hello_, _ := opts.Bool("hello"); hello_ {
		fmt.Println("Hello, World!")
	} else if test_, _ := opts.Bool("test"); test_ {
		tether.Test()
	}
}

// get the public ip of this machine as default endpoint
// (if not found then return the string "unknown")
func getPublicIP() string {
	const unknownIP = "unknown"
	resp, err := http.Get("https://api.ipify.org?format=text")
	if err != nil {
		fmt.Println("Could not get the IP, consider not using the default option.")
		return unknownIP
	}
	defer resp.Body.Close()

	ip, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Could not get the IP, consider not using the default option.")
		return unknownIP
	}

	return string(ip)
}

func upCli(opts docopt.Opts) {
	configFile, err := opts.String("--config_file")
	if err != nil {
		configFile = DefaultConfigFile
	}
	finalConfPath := configFile + tc.DeviceName + ".conf"

	bywgConf, err := tether.ParseConfig(finalConfPath)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Running up command for config %q of device %q\n", finalConfPath, tc.DeviceName)
	err = tc.BringUpInterface(bywgConf)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Interface %q brought up successfully.\n", tc.DeviceName)
}

func downCli(opts docopt.Opts) {
	configFile, err := opts.String("--config_file")
	if err != nil {
		configFile = DefaultConfigFile
	}
	finalConfPath := configFile + tc.DeviceName + ".conf"

	bywgConf, err := tether.ParseConfig(finalConfPath)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Running down command for config %q of device %q\n", finalConfPath, tc.DeviceName)
	err = tc.BringDownInterface(bywgConf)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Interface %q brought down successfully.\n", tc.DeviceName)
}

func genPrivKeyCli() {
	privateKeyGenerated, err := wgtypes.GeneratePrivateKey()
	if err != nil {
		fmt.Println("failed to generate private key: %w", err)
		return
	}
	fmt.Println(privateKeyGenerated.String())
}

func genPubKeyCli(opts docopt.Opts) {
	privateKey, err := opts.String("--priv_key")
	if err != nil {
		panic(err)
	}

	_pk, err := wgtypes.ParseKey(privateKey)
	if err != nil {
		fmt.Println("Invalid private key provided: %w", err)
		return
	}

	fmt.Println(_pk.PublicKey())
}

func getDeviceNamesCli() {
	devices, err := tc.GetAvailableDevices()
	if err != nil {
		panic(err)
	}

	if len(devices) == 0 {
		fmt.Println("No devices found.")
		return
	}

	deviceNames := make([]string, len(devices))
	for i, deviceName := range devices {
		deviceNames[i] = fmt.Sprintf("%q", deviceName)
	}

	fmt.Printf("Found %d device(s) - %s\n", len(devices), strings.Join(deviceNames, ", "))
}

func getDeviceCli() {
	config, err := tc.GetDeviceFormatted()
	if err != nil {
		panic(err)
	}

	fmt.Println(config)
}

func changeDeviceCli(opts docopt.Opts) {
	listenPort, err := opts.Int("--listen_port")
	var listenPortP *int // if listenPort is not provided then listenPortP is nil
	if err == nil {
		listenPortP = &listenPort
	}

	privateKey, err := opts.String("--priv_key")
	var privKeyP *wgtypes.Key // if privateKey is not provided then privKeyP is nil
	if err == nil {
		privKey, err := wgtypes.ParseKey(privateKey)
		if err != nil {
			fmt.Println("Invalid private key provided. Change not possible.")
			return
		}
		privKeyP = &privKey
	}

	err = tc.ChangeDevice(privKeyP, listenPortP)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Device %q changed successfully.\n", tc.DeviceName)
}

func addPeerCli(opts docopt.Opts) {
	publicKey, err := opts.String("--pub_key")
	if err != nil {
		panic(err)
	}

	_pk, err := wgtypes.ParseKey(publicKey)
	if err != nil {
		panic(err)
	}

	config, err := tc.AddPeerToDevice(_pk)
	if err != nil {
		panic(err)
	}

	fmt.Println(config)
}

func getPeerConfigCli(opts docopt.Opts) {
	publicKey, err := opts.String("--pub_key")
	if err != nil {
		panic(err)
	}

	config, err := tc.GetPeerConfig(publicKey)
	if err != nil {
		panic(err)
	}

	fmt.Println(config)
}

func startApiCli(opts docopt.Opts) {
	apiUrl, err := opts.String("--api_url")
	if err != nil {
		apiUrl = DefaultApiUrl
	}

	router := gin.Default()
	// endpoint for adding peer and returning config
	router.POST("/peer/add/*pubkey", apiAddPeer)
	// endpoint for getting existing peer config
	router.GET("/peer/config/*pubkey", apiGetPeerConfig)
	// router.Run(apiUrl)

	// wrap Gin router in an HTTP server
	srv := &http.Server{
		Addr:    apiUrl,
		Handler: router,
	}

	fmt.Printf("Starting API server at %s\n", apiUrl)

	// start server in goroutine and wait for interrupt signal
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit //  wait for a signal to be received
	fmt.Println(" Shutting down server... ")

	// try to shutdown the server gracefully (wait max 5 secs to finish pending requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	fmt.Println("Server exiting")
}

func parsePublicKey(context *gin.Context) string {
	publicKey := context.Param("pubkey")

	// remove leading slash since we are using wildcards (should always be trimmed)
	publicKey = strings.TrimPrefix(publicKey, "/")

	return publicKey
}

func apiGetPeerConfig(context *gin.Context) {
	publicKey := parsePublicKey(context)

	config, err := tc.GetPeerConfig(publicKey)
	if err != nil {
		context.String(http.StatusNotFound, fmt.Sprintf("%d Not Found - %v", http.StatusNotFound, err))
		return
	}

	// return the config as plaintext in the response body
	context.String(http.StatusOK, config)
}

func apiAddPeer(context *gin.Context) {
	publicKey := parsePublicKey(context)

	_pk, err := wgtypes.ParseKey(publicKey)
	if err != nil {
		context.String(http.StatusInternalServerError, fmt.Sprintf("%d Internal Server Error - Invalid public key %q!", http.StatusInternalServerError, publicKey))
		return
	}

	config, err := tc.AddPeerToDevice(_pk)
	if err != nil {
		context.String(http.StatusInternalServerError, fmt.Sprintf("%d Internal Server Error - %v", http.StatusInternalServerError, err))
		return
	}

	// return the config as plaintext in the response body
	context.String(http.StatusOK, config)
}

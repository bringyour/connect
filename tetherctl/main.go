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

const DefaultApiUrl = "localhost:9090"
const DefaultDeviceName = "wg0"
const DefaultListenPort = 51820

var tc tether.TetherClient

func main() {
	usage := fmt.Sprintf(
		`Tether control.

Usage:
	tetherctl setup-device [--device_name=<device_name>] 
		[--listen_port=<listen_port>]
		[--priv_key=<priv_key>]
	tetherctl get-device-names
	tetherctl get-device-interface [--device_name=<device_name>]
	tetherctl add-peer [--device_name=<device_name>] [--endpoint=<endpoint>]
		--pub_key=<pub_key> 
	tetherctl get-peer-config [--device_name=<device_name>] [--endpoint=<endpoint>]
		--pub_key=<pub_key>
    tetherctl start-api [--device_name=<device_name>] [--endpoint=<endpoint>]
		[--api_url=<api_url>]
	tetherctl hello [--device_name=<device_name>]
    
Options:
    -h --help                       Show this screen.
    --version                       Show version.
    --api_url=<api_url> 			[default: %q]
    --device_name=<device_name> 	Wireguard device name [default: %q].
	--endpoint=<endpoint> 			Wireguard url/ip where server can be found [default: this_pc_public_ip].
	--pub_key=<pub_key> 			Public key of a peer (unique)
	--listen_port=<listen_port> 	Port to listen on for incoming connections [default: %d].
	--priv_key=<priv_key> 			Private key of the device (if not given it is auto generated).

    `,
		DefaultApiUrl,
		DefaultDeviceName,
		DefaultListenPort,
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
		log.Fatalf("failed to open wgctrl: %v", err)
	}
	defer c.Close()

	tc = tether.TetherClient{
		Client:     c,
		ClientIP:   net.ParseIP(wgEndpoint),
		DeviceName: deviceName,
	}
	fmt.Printf("\033[34mTether client created for IP %q and device %q\033[0m\n\n", wgEndpoint, deviceName)

	if setupDevice_, _ := opts.Bool("setup-device"); setupDevice_ {
		setupDeviceCli(opts)
	} else if getDeviceNames_, _ := opts.Bool("get-device-names"); getDeviceNames_ {
		getDeviceNamesCli()
	} else if getDeviceInterface_, _ := opts.Bool("get-device-interface"); getDeviceInterface_ {
		getDeviceInterfaceCli()
	} else if addPeer_, _ := opts.Bool("add-peer"); addPeer_ {
		addPeerCli(opts)
	} else if getConfig_, _ := opts.Bool("get-peer-config"); getConfig_ {
		getPeerConfigCli(opts)
	} else if startAPI_, _ := opts.Bool("start-api"); startAPI_ {
		startAPICli(opts)
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

func setupDeviceCli(opts docopt.Opts) {
	listenPort, err := opts.Int("--listen_port")
	if err != nil {
		listenPort = DefaultListenPort
	}

	privateKey, err := opts.String("--priv_key")
	var privKeyP *wgtypes.Key
	if err == nil {
		privKey, err := wgtypes.ParseKey(privateKey)
		if err != nil {
			fmt.Println("Invalid private key provided. Setup not possible.")
			return
		}
		privKeyP = &privKey
	}

	err = tc.SetupDevice(listenPort, privKeyP)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Device %q setup successfully.\n", tc.DeviceName)
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

func getDeviceInterfaceCli() {
	config, err := tc.GetDeviceInterface()
	if err != nil {
		panic(err)
	}

	fmt.Println(config)
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

func startAPICli(opts docopt.Opts) {
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

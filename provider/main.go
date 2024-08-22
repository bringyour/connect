package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"syscall"

	"golang.org/x/term"

	"github.com/docopt/docopt-go"

	gojwt "github.com/golang-jwt/jwt/v5"

	"github.com/bringyour/connect/connect"
	"github.com/bringyour/connect/protocol"
)

const DefaultApiUrl = "https://api.bringyour.com"
const DefaultConnectUrl = "wss://connect.bringyour.com"

const LocalVersion = "0.0.0-local"

func main() {
	usage := fmt.Sprintf(
		`Connect provider.

The default urls are:
    api_url: %s
    connect_url: %s

Usage:
    provider provide [--port=<port>] --user_auth=<user_auth> [--password=<password>]
        [--api_url=<api_url>]
        [--connect_url=<connect_url>]
    
Options:
    -h --help                        Show this screen.
    --version                        Show version.
    --api_url=<api_url>
    --connect_url=<connect_url>
    --user_auth=<user_auth>
    --password=<password>
    -p --port=<port>   Listen port [default: 80].`,
		DefaultApiUrl,
		DefaultConnectUrl,
	)

	opts, err := docopt.ParseArgs(usage, os.Args[1:], RequireVersion())
	if err != nil {
		panic(err)
	}

	if provide_, _ := opts.Bool("provide"); provide_ {
		provide(opts)
	}
}

func provide(opts docopt.Opts) {
	port, _ := opts.Int("--port")

	var apiUrl string
	if apiUrlAny := opts["--api_url"]; apiUrlAny != nil {
		apiUrl = apiUrlAny.(string)
	} else {
		apiUrl = DefaultApiUrl
	}

	var connectUrl string
	if connectUrlAny := opts["--connect_url"]; connectUrlAny != nil {
		connectUrl = connectUrlAny.(string)
	} else {
		connectUrl = DefaultConnectUrl
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	event := connect.NewEventWithContext(cancelCtx)
	event.SetOnSignals(syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	ctx := event.Ctx()

	byClientJwt, clientId := provideAuth(ctx, apiUrl, opts)

	instanceId := connect.NewId()

	clientOob := connect.NewApiOutOfBandControl(ctx, byClientJwt, apiUrl)
	connectClient := connect.NewClientWithDefaults(ctx, clientId, clientOob)

	// routeManager := connect.NewRouteManager(connectClient)
	// contractManager := connect.NewContractManagerWithDefaults(connectClient)
	// connectClient.Setup(routeManager, contractManager)
	// go connectClient.Run()

	fmt.Printf("client_id: %s\n", clientId)
	fmt.Printf("instance_id: %s\n", instanceId)

	auth := &connect.ClientAuth{
		ByJwt: byClientJwt,
		// ClientId: clientId,
		InstanceId: instanceId,
		AppVersion: RequireVersion(),
	}
	connect.NewPlatformTransportWithDefaults(ctx, connectUrl, auth, connectClient.RouteManager())
	// go platformTransport.Run(connectClient.RouteManager())

	localUserNat := connect.NewLocalUserNatWithDefaults(ctx, clientId.String())
	remoteUserNatProvider := connect.NewRemoteUserNatProviderWithDefaults(connectClient, localUserNat)

	provideModes := map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Public:  true,
		protocol.ProvideMode_Network: true,
	}
	connectClient.ContractManager().SetProvideModes(provideModes)

	fmt.Printf(
		"Status %s on *:%d\n",
		RequireVersion(),
		port,
	)

	statusServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: &Status{},
	}

	go func() {
		defer cancel()
		err := statusServer.ListenAndServe()
		if err != nil {
			fmt.Printf("status error: %s\n", err)
		}
	}()

	select {
	case <-ctx.Done():
	}

	statusServer.Shutdown(ctx)

	remoteUserNatProvider.Close()
	localUserNat.Close()
	connectClient.Cancel()

	// exit
	os.Exit(0)
}

func provideAuth(ctx context.Context, apiUrl string, opts docopt.Opts) (byClientJwt string, clientId connect.Id) {
	userAuth := opts["--user_auth"].(string)

	var password string
	if passwordAny := opts["--password"]; passwordAny != nil {
		password = passwordAny.(string)
	} else {
		fmt.Print("Enter password: ")
		passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			panic(err)
		}
		password = string(passwordBytes)
		fmt.Printf("\n")
	}

	// fmt.Printf("userAuth='%s'; password='%s'\n", userAuth, password)

	api := connect.NewBringYourApiWithContext(ctx, apiUrl)

	loginCallback, loginChannel := connect.NewBlockingApiCallback[*connect.AuthLoginWithPasswordResult]()

	loginArgs := &connect.AuthLoginWithPasswordArgs{
		UserAuth: userAuth,
		Password: password,
	}

	api.AuthLoginWithPassword(loginArgs, loginCallback)

	var loginResult connect.ApiCallbackResult[*connect.AuthLoginWithPasswordResult]
	select {
	case <-ctx.Done():
		os.Exit(0)
	case loginResult = <-loginChannel:
	}

	if loginResult.Error != nil {
		panic(loginResult.Error)
	}
	if loginResult.Result.Error != nil {
		panic(fmt.Errorf("%s", loginResult.Result.Error.Message))
	}
	if loginResult.Result.VerificationRequired != nil {
		panic(fmt.Errorf("Verification required for %s. Use the app or web to complete account setup.", loginResult.Result.VerificationRequired.UserAuth))
	}

	api.SetByJwt(loginResult.Result.Network.ByJwt)

	authClientCallback, authClientChannel := connect.NewBlockingApiCallback[*connect.AuthNetworkClientResult]()

	authClientArgs := &connect.AuthNetworkClientArgs{
		Description: fmt.Sprintf("provider %s", RequireVersion()),
		DeviceSpec:  "",
	}

	api.AuthNetworkClient(authClientArgs, authClientCallback)

	var authClientResult connect.ApiCallbackResult[*connect.AuthNetworkClientResult]
	select {
	case <-ctx.Done():
		os.Exit(0)
	case authClientResult = <-authClientChannel:
	}

	if authClientResult.Error != nil {
		panic(authClientResult.Error)
	}
	if authClientResult.Result.Error != nil {
		panic(fmt.Errorf("%s", authClientResult.Result.Error.Message))
	}

	byClientJwt = authClientResult.Result.ByClientJwt

	// parse the clientId
	parser := gojwt.NewParser()
	token, _, err := parser.ParseUnverified(byClientJwt, gojwt.MapClaims{})
	if err != nil {
		panic(err)
	}

	claims := token.Claims.(gojwt.MapClaims)

	clientId, err = connect.ParseId(claims["client_id"].(string))
	if err != nil {
		panic(err)
	}

	return
}

type Status struct {
}

func (self *Status) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	type WarpStatusResult struct {
		Version       string `json:"version,omitempty"`
		ConfigVersion string `json:"config_version,omitempty"`
		Status        string `json:"status"`
		ClientAddress string `json:"client_address,omitempty"`
		Host          string `json:"host"`
	}

	result := &WarpStatusResult{
		Version:       RequireVersion(),
		ConfigVersion: RequireConfigVersion(),
		Status:        "ok",
		Host:          RequireHost(),
	}

	responseJson, err := json.Marshal(result)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJson)
}

func Host() (string, error) {
	host := os.Getenv("WARP_HOST")
	if host != "" {
		return host, nil
	}
	host, err := os.Hostname()
	if err == nil {
		return host, nil
	}
	return "", errors.New("WARP_HOST not set")
}

func RequireHost() string {
	host, err := Host()
	if err != nil {
		panic(err)
	}
	return host
}

func RequireVersion() string {
	if version := os.Getenv("WARP_VERSION"); version != "" {
		return version
	}
	return LocalVersion
}

func RequireConfigVersion() string {
	if version := os.Getenv("WARP_CONFIG_VERSION"); version != "" {
		return version
	}
	return LocalVersion
}

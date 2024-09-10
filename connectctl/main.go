package main

import (
	"context"
	"fmt"
	"os"

	// "os/exec"
	// "path/filepath"
	// "encoding/json"
	"time"
	// "strings"
	// "math"
	// "reflect"
	// "sort"
	// "syscall"
	// "os/signal"
	// "errors"
	// "regexp"
	"encoding/json"
	"io"
	"log"
	"net/http"

	// "encoding/base64"
	"bytes"

	// "golang.org/x/exp/maps"

	gojwt "github.com/golang-jwt/jwt/v5"

	"github.com/docopt/docopt-go"

	"bringyour.com/connect"
	"bringyour.com/protocol"
)

const ConnectCtlVersion = "0.0.1"

const DefaultApiUrl = "https://api.bringyour.com"
const DefaultConnectUrl = "wss://connect.bringyour.com"

var Out *log.Logger
var Err *log.Logger

func init() {
	Out = log.New(os.Stdout, "", 0)
	Err = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	usage := fmt.Sprintf(
		`Connect control.

The default urls are:
    api_url: %s
    connect_url: %s

Usage:
    connectctl create-network [--api_url=<api_url>]
        --network_name=<network_name>
        --user_name=<user_name>
        --user_auth=<user_auth>
        --password=<password>
    connectctl verify-send [--api_url=<api_url>]
        --user_auth=<user_auth>
    connectctl verify-network [--api_url=<api_url>]
        --user_auth=<user_auth>
        --code=<code>
    connectctl login-network [--api_url=<api_url>]
        --user_auth=<user_auth>
        --password=<password>
    connectctl verify-network [--api_url=<api_url>]
        --user_auth=<user_auth>
        --code=<code>
    connectctl client-id [--api_url=<api_url>] --jwt=<jwt> 
    connectctl send [--connect_url=<connect_url>] [--api_url=<api_url>] --jwt=<jwt>
        --destination_id=<destination_id>
        <message>
        [--message_count=<message_count>]
        [--instance_id=<instance_id>]
    connectctl sink [--connect_url=<connect_url>] [--api_url=<api_url>] --jwt=<jwt>
        [--message_count=<message_count>]
        [--instance_id=<instance_id>]
    
Options:
    -h --help                        Show this screen.
    --version                        Show version.
    --api_url=<api_url>
    --connect_url=<connect_url>
    --network_name=<network_name>
    --user_name=<user_name>
    --user_auth=<user_auth>
    --password=<password>
    --code=<code>
    --jwt=<jwt>                      Your platform JWT.
    --destination_id=<destination_id>   Destination client_id
    --message_count=<message_count>  Print this many messages then exit.
    --instance_id=<instance_id>      Set the client instance id.`,
		DefaultApiUrl,
		DefaultConnectUrl,
	)

	opts, err := docopt.ParseArgs(usage, os.Args[1:], ConnectCtlVersion)
	if err != nil {
		panic(err)
	}

	if createNetwork_, _ := opts.Bool("create-network"); createNetwork_ {
		createNetwork(opts)
	} else if verifySend_, _ := opts.Bool("verify-send"); verifySend_ {
		verifySend(opts)
	} else if verifyNetwork_, _ := opts.Bool("verify-network"); verifyNetwork_ {
		verifyNetwork(opts)
	} else if loginNetwork_, _ := opts.Bool("login-network"); loginNetwork_ {
		loginNetwork(opts)
	} else if clientId_, _ := opts.Bool("client-id"); clientId_ {
		clientId(opts)
	} else if send_, _ := opts.Bool("send"); send_ {
		send(opts)
	} else if sink_, _ := opts.Bool("sink"); sink_ {
		sink(opts)
	}
}

func printResult(result map[string]any) {
	expandByJwt(result)

	out, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", out)
}

func expandByJwt(result map[string]any) {
	if jwt, ok := result["by_jwt"]; ok {
		claims := gojwt.MapClaims{}
		gojwt.NewParser().ParseUnverified(jwt.(string), claims)

		for claimKey, claimValue := range claims {
			result[fmt.Sprintf("by_jwt_%s", claimKey)] = claimValue
		}
	}
	if jwt, ok := result["by_client_jwt"]; ok {
		claims := gojwt.MapClaims{}
		gojwt.NewParser().ParseUnverified(jwt.(string), claims)

		for claimKey, claimValue := range claims {
			result[fmt.Sprintf("by_client_jwt_%s", claimKey)] = claimValue
		}
	}
	for _, value := range result {
		if subResult, ok := value.(map[string]any); ok {
			expandByJwt(subResult)
		}
	}
}

func createNetwork(opts docopt.Opts) {
	apiUrl, err := opts.String("--api_url")
	if err != nil {
		apiUrl = DefaultApiUrl
	}

	networkName, _ := opts.String("--network_name")

	userName, _ := opts.String("--user_name")

	userAuth, _ := opts.String("--user_auth")

	password, _ := opts.String("--password")

	timeout := 5 * time.Second

	// /auth/network-create
	args := map[string]any{}
	args["user_name"] = userName
	args["user_auth"] = userAuth
	args["password"] = password
	args["network_name"] = networkName
	args["terms"] = true

	reqBody, err := json.Marshal(args)

	// fmt.Printf("request: %s\n", reqBody)

	req, err := http.NewRequest(
		"POST",
		fmt.Sprintf("%s/auth/network-create", apiUrl),
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: timeout,
	}

	res, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}

	// fmt.Printf("response: %s\n", resBody)

	result := map[string]any{}
	err = json.Unmarshal(resBody, &result)
	if err != nil {
		panic(err)
	}

	printResult(result)
}

func verifySend(opts docopt.Opts) {
	apiUrl, err := opts.String("--api_url")
	if err != nil {
		apiUrl = DefaultApiUrl
	}

	userAuth, _ := opts.String("--user_auth")

	timeout := 5 * time.Second

	// /auth/verify-send
	args := map[string]any{}
	args["user_auth"] = userAuth

	reqBody, err := json.Marshal(args)

	req, err := http.NewRequest(
		"POST",
		fmt.Sprintf("%s/auth/verify-send", apiUrl),
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: timeout,
	}

	res, err := client.Do(req)
	if err != nil {
		panic(err)
		return
	}
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err)
		return
	}

	result := map[string]any{}
	err = json.Unmarshal(resBody, &result)
	if err != nil {
		panic(err)
		return
	}

	printResult(result)
}

func verifyNetwork(opts docopt.Opts) {
	apiUrl, err := opts.String("--api_url")
	if err != nil {
		apiUrl = DefaultApiUrl
	}

	userAuth, _ := opts.String("--user_auth")

	verifyCode, _ := opts.String("--code")

	timeout := 5 * time.Second

	// /auth/verify
	args := map[string]any{}
	args["user_auth"] = userAuth
	args["verify_code"] = verifyCode

	reqBody, err := json.Marshal(args)

	req, err := http.NewRequest(
		"POST",
		fmt.Sprintf("%s/auth/verify", apiUrl),
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: timeout,
	}

	res, err := client.Do(req)
	if err != nil {
		panic(err)
		return
	}
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err)
		return
	}

	result := map[string]any{}
	err = json.Unmarshal(resBody, &result)
	if err != nil {
		panic(err)
		return
	}

	printResult(result)
}

func loginNetwork(opts docopt.Opts) {
	apiUrl, err := opts.String("--api_url")
	if err != nil {
		apiUrl = DefaultApiUrl
	}

	userAuth, _ := opts.String("--user_auth")

	password, _ := opts.String("--password")

	timeout := 5 * time.Second

	// /auth/login-with-password
	args := map[string]any{}
	args["user_auth"] = userAuth
	args["password"] = password

	reqBody, err := json.Marshal(args)

	req, err := http.NewRequest(
		"POST",
		fmt.Sprintf("%s/auth/login-with-password", apiUrl),
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: timeout,
	}

	res, err := client.Do(req)
	if err != nil {
		panic(err)
		return
	}
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err)
		return
	}

	result := map[string]any{}
	err = json.Unmarshal(resBody, &result)
	if err != nil {
		panic(err)
		return
	}

	printResult(result)
}

// use the given jwt to generate a new jwt with a new client id
func clientId(opts docopt.Opts) {
	apiUrl, err := opts.String("--api_url")
	if err != nil {
		apiUrl = DefaultApiUrl
	}

	jwt, _ := opts.String("--jwt")

	bearer := []byte(jwt)

	timeout := 5 * time.Second

	// /network/auth-client
	args := map[string]any{}
	args["description"] = ""
	args["device_spec"] = ""

	reqBody, err := json.Marshal(args)

	// fmt.Printf("request: %s\n", reqBody)

	req, err := http.NewRequest(
		"POST",
		fmt.Sprintf("%s/network/auth-client", apiUrl),
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", bearer))

	client := &http.Client{
		Timeout: timeout,
	}

	res, err := client.Do(req)
	if err != nil {
		panic(err)
		return
	}
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err)
		return
	}

	fmt.Printf("response: %s\n", resBody)

	result := map[string]any{}
	err = json.Unmarshal(resBody, &result)
	if err != nil {
		panic(err)
		return
	}

	printResult(result)
}

func send(opts docopt.Opts) {
	jwt, _ := opts.String("--jwt")

	var clientId connect.Id

	claims := gojwt.MapClaims{}
	gojwt.NewParser().ParseUnverified(jwt, claims)

	jwtClientId, ok := claims["client_id"]
	if !ok {
		fmt.Printf("JWT does not have a client_id.\n")
		return
	}
	switch v := jwtClientId.(type) {
	case string:
		var err error
		clientId, err = connect.ParseId(v)
		if err != nil {
			fmt.Printf("JWT has invalid client_id (%s).\n", err)
			return
		}
	default:
		fmt.Printf("JWT has invalid client_id (%T).\n", v)
		return
	}

	fmt.Printf("client_id: %s\n", clientId.String())

	connectUrl, err := opts.String("--connect_url")
	if err != nil {
		connectUrl = DefaultConnectUrl
	}
	apiUrl, err := opts.String("--api_url")
	if err != nil {
		apiUrl = DefaultApiUrl
	}

	destinationIdStr, _ := opts.String("--destination_id")
	destinationId, err := connect.ParseId(destinationIdStr)
	if err != nil {
		fmt.Printf("Invalid destination_id (%s).\n", err)
		return
	}

	instanceIdStr, err := opts.String("--instance_id")
	var instanceId connect.Id
	if err == nil {
		instanceId, err = connect.ParseId(instanceIdStr)
		if err != nil {
			fmt.Printf("Invalid instance_id (%s).\n", err)
			return
		}
	} else {
		instanceId = connect.NewId()
	}

	fmt.Printf("instance_id: %s\n", instanceId.String())

	messageContent, _ := opts.String("<message>")

	messageCount, err := opts.Int("--message_count")
	if err != nil {
		messageCount = 1
	}

	// need at least one. Use more for testing.
	transportCount := 4

	timeout := 30 * time.Second

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	api := connect.NewBringYourApi(connect.DefaultClientStrategy(cancelCtx), apiUrl)
	api.SetByJwt(jwt)
	oobControl := connect.NewApiOutOfBandControlWithApi(api)

	client := connect.NewClientWithDefaults(
		cancelCtx,
		clientId,
		oobControl,
		connect.NewApiWebRTCConnProvider(
			cancelCtx,
			connect.DefaultClientStrategy(cancelCtx),
			jwt,
			apiUrl,
			clientId,
		),
	)
	defer client.Close()

	// client.SetInstanceId(instanceId)

	// routeManager := connect.NewRouteManager(client)
	// contractManager := connect.NewContractManagerWithDefaults(client)
	// client.Setup(routeManager, contractManager)
	// go client.Run()

	auth := &connect.ClientAuth{
		ByJwt:      jwt,
		InstanceId: instanceId,
		AppVersion: fmt.Sprintf("connectctl %s", ConnectCtlVersion),
	}
	for i := 0; i < transportCount; i += 1 {
		platformTransport := connect.NewPlatformTransportWithDefaults(
			cancelCtx,
			connect.DefaultClientStrategy(cancelCtx),
			client.RouteManager(),
			fmt.Sprintf("%s/", connectUrl),
			auth,
		)
		defer platformTransport.Close()
		// go platformTransport.Run(routeManager)
	}

	provideModes := map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
	}
	client.ContractManager().SetProvideModes(provideModes)

	// FIXME break into 2k chunks?
	acks := make(chan error)
	go func() {
		for i := 0; i < messageCount; i += 1 {
			var content string
			if 0 < messageCount {
				content = fmt.Sprintf("[%d] %s", i, messageContent)
			} else {
				content = messageContent
			}
			message := &protocol.SimpleMessage{
				Content: content,
			}

			client.Send(
				connect.RequireToFrame(message),
				connect.DestinationId(destinationId),
				func(err error) {
					acks <- err
				},
			)
		}
	}()
	for i := 0; i < messageCount; i += 1 {
		select {
		case err := <-acks:
			if err == nil {
				fmt.Printf("Message acked.\n")
			} else {
				fmt.Printf("Message not acked (%s).\n", err)
			}
		case <-time.After(timeout):
			fmt.Printf("Message not acked (timeout).\n")
		}
	}
}

func sink(opts docopt.Opts) {
	jwt, _ := opts.String("--jwt")

	var clientId connect.Id

	claims := gojwt.MapClaims{}
	gojwt.NewParser().ParseUnverified(jwt, claims)

	jwtClientId, ok := claims["client_id"]
	if !ok {
		fmt.Printf("JWT does not have a client_id.\n")
		return
	}
	switch v := jwtClientId.(type) {
	case string:
		var err error
		clientId, err = connect.ParseId(v)
		if err != nil {
			fmt.Printf("JWT has invalid client_id (%s).\n", err)
			return
		}
	default:
		fmt.Printf("JWT has invalid client_id (%T).\n", v)
		return
	}

	fmt.Printf("client_id: %s\n", clientId.String())

	connectUrl, err := opts.String("--connect_url")
	if err != nil {
		connectUrl = DefaultConnectUrl
	}
	apiUrl, err := opts.String("--api_url")
	if err != nil {
		apiUrl = DefaultApiUrl
	}

	messageCount, err := opts.Int("--message_count")
	if err != nil {
		messageCount = -1
	}

	transportCount := 4

	instanceIdStr, err := opts.String("--instance_id")
	var instanceId connect.Id
	if err == nil {
		instanceId, err = connect.ParseId(instanceIdStr)
		if err != nil {
			fmt.Printf("Invalid instance_id (%s).\n", err)
			return
		}
	} else {
		instanceId = connect.NewId()
	}

	fmt.Printf("instance_id: %s\n", instanceId.String())

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	api := connect.NewBringYourApi(connect.DefaultClientStrategy(cancelCtx), apiUrl)
	api.SetByJwt(jwt)
	oobControl := connect.NewApiOutOfBandControlWithApi(api)

	client := connect.NewClientWithDefaults(
		cancelCtx,
		clientId,
		oobControl,
		connect.NewApiWebRTCConnProvider(
			cancelCtx,
			connect.DefaultClientStrategy(cancelCtx),
			jwt,
			apiUrl,
			clientId,
		),
	)
	defer client.Close()

	// client.SetInstanceId(instanceId)

	// routeManager := connect.NewRouteManager(client)
	// contractManager := connect.NewContractManagerWithDefaults(client)

	// client.Setup(routeManager, contractManager)
	// go client.Run()

	provideModes := map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
	}
	client.ContractManager().SetProvideModes(provideModes)

	auth := &connect.ClientAuth{
		ByJwt:      jwt,
		InstanceId: instanceId,
		AppVersion: fmt.Sprintf("connectctl %s", ConnectCtlVersion),
	}
	for i := 0; i < transportCount; i += 1 {
		platformTransport := connect.NewPlatformTransportWithDefaults(
			cancelCtx,
			connect.DefaultClientStrategy(cancelCtx),
			client.RouteManager(),
			fmt.Sprintf("%s/", connectUrl),
			auth,
		)
		defer platformTransport.Close()
		// go platformTransport.Run(routeManager)
	}

	type Receive struct {
		source      connect.TransferPath
		frames      []*protocol.Frame
		provideMode protocol.ProvideMode
	}

	receives := make(chan *Receive)

	client.AddReceiveCallback(func(source connect.TransferPath, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
		receives <- &Receive{
			source:      source,
			frames:      frames,
			provideMode: provideMode,
		}
	})

	// FIXME reassemble the chunks. Only a complete message counts as 1 against the message count
	for i := 0; messageCount < 0 || i < messageCount; i += 1 {
		select {
		case receive := <-receives:
			fmt.Printf("[%s %s] %s\n", receive.source, receive.provideMode, receive.frames)
		}
	}
}

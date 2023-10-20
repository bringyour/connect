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
    "log"

    // "golang.org/x/exp/maps"
    // "golang.org/x/exp/slices"

    gojwt "github.com/golang-jwt/jwt/v5"

    "github.com/docopt/docopt-go"

    "bringyour.com/connect"
    "bringyour.com/protocol"
)


const ConnectCtlVersion = "0.0.1"


var Out *log.Logger
var Err *log.Logger

func init() {
    Out = log.New(os.Stdout, "", 0)
    Err = log.New(os.Stderr, "", log.Ldate | log.Ltime | log.Lshortfile)
}


func main() {
    usage := `Connect control.

The default urls are:
    api_url: https://api.bringyour.com
    connect_url: https://connect.bringyour.com

Usage:
    connectctl create-network [--api_url=<api_url>]
        --network_name=<name>
        --name=<name>
        --user_auth=<user_auth>
        --password=<password>
    connectctl verify-network [--api_url=<api_url>] <code>
    connectctl login-network [--api_url=<api_url>]
        --user_auth=<user_auth>
        --password=<password>
    connectctl verify-network [--api_url=<api_url>] <code>
    connectctl client-id [--api_url=<api_url>] --jwt=<jwt> 
    connectctl send [--connect_url=<connect_url>] --jwt=<jwt>
        --destination=<destination_id>
        [<message>]
    connectctl sink [--connect_url=<connect_url>] --jwt=<jwt>
        [--message_count=<message_count>]
    
Options:
    -h --help                        Show this screen.
    --version                        Show version.
    --api_url=<api_url>
    --connect_url=<connect_url>
    --network_name=<name>
    --name=<name>
    --user_auth=<user_auth>
    --password=<password>
    --jwt=<jwt>                      Your platform JWT.
    --destination=<destination_id>   Destination client_id
    --message_count=<message_count>  Print this many messages then exit.`

    opts, err := docopt.ParseArgs(usage, os.Args[1:], ConnectCtlVersion)
    if err != nil {
        panic(err)
    }

    if createNetwork_, _ := opts.Bool("create-network"); createNetwork_ {
        createNetwork(opts)
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


func createNetwork(opts docopt.Opts) {

    // post create network
}


func verifyNetwork(opts docopt.Opts) {
    // post verify code
    // print jwt

}


func loginNetwork(opts docopt.Opts) {
    // login
    // print jwt

}


// use the given jwt to generate a new jwt with a new client id
func clientId(opts docopt.Opts) {
    jwt, _ := opts.String("--jwt")

    // TODO make post to client route to attach a new client id to the jwt

    log.Printf("%s", jwt)
}


// send a message
func send(opts docopt.Opts) {
    jwt, _ := opts.String("--jwt")

    destinationIdStr, _ := opts.String("--destination_id")

    messageContent, _ := opts.String("message")


    connectUrl := ""

    timeout := 30 * time.Second


    destinationId, err := connect.ParseId(destinationIdStr)
    if err != nil {
        fmt.Printf("Invalid destination_id (%s).\n", err)
        return
    }


    // TODO create a client
    // TODO create a transport to platform using the jwt
    // TODO send a message


    cancelCtx, cancel := context.WithCancel(context.Background())
    defer cancel()



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
        clientId, err = connect.ParseId(v)
        if err != nil {
            fmt.Printf("JWT has invalid client_id (%s).\n", err)
            return
        }
    default:
        fmt.Printf("JWT has invalid client_id (%T).\n", v)
        return
    }
    


    client := connect.NewClientWithDefaults(
        cancelCtx,
        clientId,
    )
    defer client.Close()

    routeManager := connect.NewRouteManager(client)
    contractManager := connect.NewContractManagerWithDefaults(client)

    go client.Run(routeManager, contractManager)

    auth := &connect.ClientAuth{
        ByJwt: jwt,
        InstanceId: client.InstanceId(),
        AppVersion: fmt.Sprintf("connectctl %s", ConnectCtlVersion),
    }
    platformTransport := connect.NewPlatformTransportWithDefaults(
        cancelCtx,
        connectUrl,
        auth,
    )
    defer platformTransport.Close()

    go platformTransport.Run(routeManager)


    ack := make(chan error)



    message := &protocol.SimpleMessage{
        Content: messageContent,
    }

    client.Send(
        connect.RequireToFrame(message),
        destinationId,
        func(err error) {
            ack <- err
        },
    )

    select {
    case err := <- ack:
        if err == nil {
            fmt.Printf("Message acked.")
        } else {
            fmt.Printf("Message not acked (%s).", err)
        }
    case <- time.After(timeout):
        fmt.Printf("Message not acked (timeout).")
    }
}


// listen for messages
func sink(opts docopt.Opts) {
    jwt, _ := opts.String("--jwt")

    var messageCount int
    if messageCount_, err := opts.Int("--message_count"); err == nil {
        messageCount = messageCount_
    } else {
        messageCount = -1
    }



    // TODO create a client
    // TODO create a transport to platform using the jwt
    // TODO listen for messages

    log.Printf("%s %d", jwt, messageCount)
}

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
    // "io"
    "net"
    "net/url"
    // "net/http"
    "log"
    // "encoding/json"
    // "encoding/base64"
    // "bytes"
    "strconv"

    // "golang.org/x/exp/maps"

    // gojwt "github.com/golang-jwt/jwt/v5"

    "github.com/docopt/docopt-go"

    "bringyour.com/connect"
    // "bringyour.com/connect"
    // "bringyour.com/protocol"
)


// run [secret] [allowed host] [port]

// probe top-mail [ip] [secret] [api host]
// probe top-web [ip] [secret] [api host]


const ExtenderVersion = "0.0.1"

const DefaultApiUrl = "https://api.bringyour.com"
const DefaultConnectUrl = "wss://connect.bringyour.com"


var Out *log.Logger
var Err *log.Logger

func init() {
    Out = log.New(os.Stdout, "", 0)
    Err = log.New(os.Stderr, "", log.Ldate | log.Ltime | log.Lshortfile)
}


func main() {
    usage := fmt.Sprintf(
        `Connect extender.

The default urls are:
    api_url: %s
    connect_url: %s

Usage:
    extender server
    	[--secret=<secret>...]
    	[--port=<port>...]
    	[--host=<host>...]
    extender probe
    	[--extender_ip=<extender_ip>...]
    	[--extender_secret=<extender_secret>]
    	[--api_url=<api_url>]
    	[--delay=<delay>]
        [--spoof_host=<spoof_host>...]
        [--spoof_port=<spoof_port>...]
    
Options:
    -h --help                        Show this screen.
    --version                        Show version.
    --api_url=<api_url>
    --delay=<delay>					 Delay with time units: ms, s, m
    --extender_secret=<extender_secret>
    --extender_ip=<extender_ip>
    --secret=<secret>
    --port=<port>
    --host=<host>    If omitted, allows the default api and connect hosts.
    --spoof_host=<spoof_host>`,
        DefaultApiUrl,
        DefaultConnectUrl,
    )

    opts, err := docopt.ParseArgs(usage, os.Args[1:], ExtenderVersion)
    if err != nil {
        panic(err)
    }

    if server_, _ := opts.Bool("server"); server_ {
        server(opts)
    } else if probe_, _ := opts.Bool("probe"); probe_ {
    	probe(opts)
    }

}


func server(opts docopt.Opts) {

    standardPorts := []int{
        // https and secure dns
        443,
        // dns
        853,
        // ldap
        636,
        // ftp
        989,
        990,
        // telnet
        992,
        // irc
        994,
        // docker
        2376,
        // ldap
        3269,
        // sip
        5061,
        // powershell
        5986,
        // alt https
        8443,
        // tor
        9001,
        // imap
        993,
        // pop
        995,
        // smtp
        465,
        // ntp, nts
        4460,
        // cpanel
        2083,
        2096,
        // webhost mail
        2087,
    }



    allowedSecrets := []string{}
    allowedHosts := []string{}
    ports := []int{}


    if secretStrs, ok := opts["--secret"]; ok {
        if v, ok := secretStrs.([]string); ok {
            allowedSecrets = append(allowedSecrets, v...)
        }
    }

    if hostStrs, ok := opts["--host"]; ok {
        if v, ok := hostStrs.([]string); ok {
            allowedHosts = append(allowedHosts, v...)
        }
    }

    if portStrs, ok := opts["--port"]; ok {
        if v, ok := portStrs.([]string); ok {
            for _, portStr := range v {
                if port, err := strconv.Atoi(portStr); err == nil {
                    ports = append(ports, port)
                }
            }
        }
    }

    if len(allowedHosts) == 0 {
        allowedHosts = append(allowedHosts, RequireHost(DefaultApiUrl), RequireHost(DefaultConnectUrl))
    }

    if len(ports) == 0 {
        ports = append(ports, standardPorts...)
    }


    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()




    forwardDialer := &net.Dialer{}

    for _, port := range ports {
        fmt.Printf("Listening on *:%d\n", port)
    }

    extenderServer := connect.NewExtenderServer(
        ctx,
        allowedSecrets,
        allowedHosts,
        ports,
        forwardDialer,
    )
    defer extenderServer.Close()
    extenderServer.ListenAndServe()
}


func probe(opts docopt.Opts) {

    connectMode := connect.ExtenderConnectModeQuic

    extenderIps := []net.IP{}
    extenderSecret := ""
    apiUrl := DefaultApiUrl
    scanDelay := 50 * time.Millisecond
    spoofHosts := []string{}
    spoofPorts := []int{}

    if extenderIpStrs, ok := opts["--extender_ip"]; ok {
        if v, ok := extenderIpStrs.([]string); ok {
            for _, extenderIpStr := range v {
                if extenderIp := net.ParseIP(extenderIpStr); extenderIp != nil {
                    extenderIps = append(extenderIps, extenderIp)
                } else {
                    fmt.Printf("Bad extender IP: %s\n", extenderIpStr)
                    return
                }
            }
        }
    }

    if extenderSecretStr, ok := opts["--extender_secret"]; ok {
        if v, ok := extenderSecretStr.(string); ok {
            extenderSecret = v
        }
    }

    if apiUrlStr, ok := opts["--apiUrl"]; ok {
        if v, ok := apiUrlStr.(string); ok {
            apiUrl = v
        }
    }

    if delayStr, ok := opts["--delay"]; ok {
        if v, ok := delayStr.(string); ok {
            if d, err := time.ParseDuration(v); err == nil {
                scanDelay = d
            }
        }
    }

    if spoofHostsStrs, ok := opts["--spoof_host"]; ok {
        if v, ok := spoofHostsStrs.([]string); ok {
            spoofHosts = append(spoofHosts, v...)
        }
    }

    if spoofPortStrs, ok := opts["--spoof_port"]; ok {
        if v, ok := spoofPortStrs.([]string); ok {
            for _, spoofPortStr := range v {
                if spoofPort, err := strconv.Atoi(spoofPortStr); err == nil {
                    spoofPorts = append(spoofPorts, spoofPort)
                }
            }
        }
    }


    callback := func(config *connect.ExtenderConfig, success bool) {
        if success {
            for _, spoofHost := range config.SpoofHosts {
                for _, extenderIp := range config.ExtenderIps {
                    for _, extenderPort := range config.ExtenderPorts {
                        fmt.Printf("%s -> %s:%d\n", spoofHost, extenderIp, extenderPort)
                    }
                }
            }
        } else {
            fmt.Printf(".")
            // fmt.Printf("NO %s -> %s:%d\n", config.SpoofHost, config.ExtenderIp.String(), config.ExtenderPort)
        }
    }

    if 0 < len(spoofHosts) {
        if len(spoofPorts) == 0 {
            standardPorts := []int{
                // https and secure dns
                443,
                // dns
                853,
                // ldap
                636,
                // ftp
                989,
                990,
                // telnet
                992,
                // irc
                994,
                // docker
                2376,
                // ldap
                3269,
                // sip
                5061,
                // powershell
                5986,
                // alt https
                8443,
                // tor
                9001,
                // imap
                993,
                // pop
                995,
                // smtp
                465,
                // ntp, nts
                4460,
                // cpanel
                2083,
                2096,
                // webhost mail
                2087,
            }
            spoofPorts = append(spoofPorts, standardPorts...)
        }

        SearchExtenders(
            connectMode,
            spoofHosts,
            extenderIps,
            extenderSecret,
            spoofPorts,
            apiUrl,
            scanDelay,
            callback,
        )


    } else {

        SearchMailExtenders(
            connectMode,
            extenderIps,
            extenderSecret,
            apiUrl,
            scanDelay,
            callback,
        )

        SearchWebExtenders(
            connectMode,
            extenderIps,
            extenderSecret,
            apiUrl,
            scanDelay,
            callback,
        )
    }

}


func Host(urlStr string) (string, error) {
    url, err := url.Parse(urlStr)
    if err != nil {
        return "", err
    }
    return url.Hostname(), nil
}


func RequireHost(urlStr string) string {
    host, err := Host(urlStr)
    if err != nil {
        panic(err)
    }
    return host
}


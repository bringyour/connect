package main

import (
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/docopt/docopt-go"

	"bringyour.com/connect/tether"
	"bringyour.com/wireguard/logger"
)

const TetherCtlVersion = "0.0.1"

var tc tether.Client

var l = logger.NewLogger(logger.LogLevelVerbose, "(tetherctl) ") // global logger

func main() {
	usage := `Tether control.

Usage:
    tetherctl build (--test | -t) [--log=<log>]
    tetherctl cli
    
Options:
    -h --help               Show this screen.
    --version               Show version.
    -t --test               Test mode.
	--log=<log>             Log level from verbose(v), error(e) and silent(s) [default: error].
    `

	tc = *tether.New() // create client

	// add ipv4 endpoint for peer
	ipv4, err := getPublicIP(true)
	if err != nil {
		l.Errorf("Failed to get peer endpoint")
		return
	}
	if err := tc.AddEndpoint(tether.EndpointIPv4, ipv4); err != nil {
		panic(err)
	}

	opts, err := docopt.ParseArgs(usage, os.Args[1:], TetherCtlVersion)
	if err != nil {
		panic(err)
	}

	if build_, _ := opts.Bool("build"); build_ {
		build(opts)
	} else if cli_, _ := opts.Bool("cli"); cli_ {
		cli()
	}
}

// getLogLevel returns the log level from the log string.
//
// Log levels include silent(s), verbose(v)/debug(d) and error(e).
// Default log level is error.
func getLogLevel(log string) int {
	logL := strings.ToLower(log)
	switch logL {
	case "verbose", "debug", "v", "d":
		return logger.LogLevelVerbose
	case "silent", "s":
		return logger.LogLevelSilent
	default:
		return logger.LogLevelError
	}
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

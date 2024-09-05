package main

import (
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

package main


import (
    // "fmt"
    "os"
    // "os/exec"
    // "path/filepath"
    // "encoding/json"
    // "time"
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

    "github.com/docopt/docopt-go"
)

// --jwt <jwt> client create
// client ls
// client rm


// --jwt <jwt> sink
// --jwt <jwt> send --destination <client_id> <message>

// forward or drop all incoming messages to a random client
// --jwt <jwt> bounce <client_id>...



// connectctl --jwt <jwt> client-id
// gets a new jwt with a new client id


// and end to end bounce test that sets up clients and floods them with messages that bounce and sink
// only one of c clients is a sink
// connectctl test-bounce -c <client count> -n <message count> -s <message size>


// TODO allow copy paste of the JWT out of the web ui
// TODO support multiple decoding keys for the JWT, and use the latest one for signing



const ConnectCtlVersion = "0.0.1"

var Out *log.Logger
var Err *log.Logger

func init() {
    Out = log.New(os.Stdout, "", 0)
    Err = log.New(os.Stderr, "", log.Ldate | log.Ltime | log.Lshortfile)
}



func main() {
	usage := `Connect control.

Usage:
    connectctl client-id --jwt=<jwt> 
    connectctl send --jwt=<jwt> --destination=<destination_id> <message>
    connectctl sink --jwt=<jwt> [--message_count=<message_count>]
    connectctl bounce --jwt=<jwt> --destination=<destination_id>... [--message_count=<message_count>]
    connectctl test-bounce --jwt=<jwt> --client_count=<client_count> --message_size=<message_size> [--message_count=<message_count>]
    
Options:
    -h --help                  Show this screen.
    --version                  Show version.
    --jwt=<jwt>                      Your platform JWT.
    --destination=<destination_id>			   Destination client_id
    --message_count=<message_count>			   Process this many messages then exit.
    --client_count=<client_count>             Number of parallel clients.
    --message_size=<message_size>             Bytes per message. Can use suffixes: b, kib, mib`

    opts, err := docopt.ParseArgs(usage, os.Args[1:], ConnectCtlVersion)
    if err != nil {
        panic(err)
    }

    if clientId_, _ := opts.Bool("clientid"); clientId_ {
        clientId(opts)
    } else if send_, _ := opts.Bool("send"); send_ {
    	send(opts)
    } else if sink_, _ := opts.Bool("sink"); sink_ {
    	sink(opts)
    } else if bounce_, _ := opts.Bool("bounce"); bounce_ {
    	bounce(opts)
    } else if testBounce_, _ := opts.Bool("test-bounce"); testBounce_ {
    	testBounce(opts)
    }
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

	destinationId, _ := opts.String("--destination_id")

	message, _ := opts.String("message")

	// TODO create a client
	// TODO create a transport to platform using the jwt
	// TODO send a message

	log.Printf("%s %s %s", jwt, destinationId, message)
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


// forward messages to a random choice of one destination
func bounce(opts docopt.Opts) {
	jwt, _ := opts.String("--jwt")

	destinationIds, _ := opts.String("--destination_id")

	message, _ := opts.String("message")

	var messageCount int
	if messageCount_, err := opts.Int("--message_count"); err == nil {
		messageCount = messageCount_
	} else {
		messageCount = -1
	}

	log.Printf("%s %s %s %d", jwt, destinationIds, message, messageCount)

}


// end to end test that creates bounce and sink clients, and generates messages
// this is a stress test that tests 1. system throughput, and 2. correct delivery of messages
func testBounce(opts docopt.Opts) {

	jwt, _ := opts.String("--jwt")

	clientCount, _ := opts.Int("--client_count")
	messageSize, _ := opts.Int("--message_size")

	var messageCount int
	if messageCount_, err := opts.Int("--message_count"); err == nil {
		messageCount = messageCount_
	} else {
		messageCount = -1
	}

	log.Printf("%s %d %d %d", jwt, clientCount, messageSize, messageCount)
}







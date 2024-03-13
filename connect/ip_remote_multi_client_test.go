package connect

import (
	"context"
    "testing"
    // "time"
    // mathrand "math/rand"
    // "fmt"
    // "crypto/hmac"
	// "crypto/sha256"
	// "sync"
	"slices"
	// "bytes"
	// "encoding/binary"
	// "net"
	// "reflect"
	// "fmt"
	// "sync"

	// "github.com/google/gopacket"
    // "github.com/google/gopacket/layers"

	// "google.golang.org/protobuf/proto"

    // "github.com/go-playground/assert/v2"

    // "bringyour.com/protocol"
)



func TestMultiClientUdp4(t *testing.T) {
	testClient(t, testingNewMultiClient, udp4Packet, (*IpPath).ToIp4Path)
}

func TestMultiClientTcp4(t *testing.T) {
	testClient(t, testingNewMultiClient, tcp4Packet, (*IpPath).ToIp4Path)
}

func TestMultiClientUdp6(t *testing.T) {
	testClient(t, testingNewMultiClient, udp6Packet, (*IpPath).ToIp6Path)
}

func TestMultiClientTcp6(t *testing.T) {
	testClient(t, testingNewMultiClient, tcp6Packet, (*IpPath).ToIp6Path)
}



func testingNewMultiClient(ctx context.Context, providerClient *Client, receivePacketCallback ReceivePacketFunction) (UserNatClient, error) {

	
	generator := &TestMultiClientGenerator{
		nextDestintationIds: func(count int, excludedClientIds []Id) (map[Id]ByteCount, error) {
			next := map[Id]ByteCount{}
			if !slices.Contains(excludedClientIds, providerClient.ClientId()) {
				 next[providerClient.ClientId()] = ByteCount(0)
			}
			return next, nil
		},
	    newClientArgs: func()(*MultiClientGeneratorClientArgs, error) {
	    	args := &MultiClientGeneratorClientArgs{
	    		ClientId: NewId(),
				ClientAuth: nil,
			}
			return args, nil
	    },
	    removeClientArgs: func(args *MultiClientGeneratorClientArgs) {
	    	// do nothing
	    },
	    newClient: func(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
	    	client := NewClientWithDefaults(ctx, args.ClientId)

	    	routesSend := []Route{
				make(chan []byte),
			}
			routesReceive := []Route{
				make(chan []byte),
			}

	    	transportSend := NewSendGatewayTransport()
			transportReceive := NewReceiveGatewayTransport()
			client.RouteManager().UpdateTransport(transportSend, routesSend)
			client.RouteManager().UpdateTransport(transportReceive, routesReceive)

			providerTransportSend := NewSendClientTransport(args.ClientId)
			providerTransportReceive := NewReceiveGatewayTransport()
			providerClient.RouteManager().UpdateTransport(providerTransportReceive, routesSend)
			providerClient.RouteManager().UpdateTransport(providerTransportSend, routesReceive)
			

			return client, nil
	    },
	}


	multiClient := NewRemoteUserNatMultiClientWithDefaults(
		ctx,
		generator,
		receivePacketCallback,
	)

	return multiClient, nil


	
}


type TestMultiClientGenerator struct {
	nextDestintationIds func(count int, excludedClientIds []Id) (map[Id]ByteCount, error)
    newClientArgs func()(*MultiClientGeneratorClientArgs, error)
    removeClientArgs func(args *MultiClientGeneratorClientArgs)
    newClient func(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error)
}

func (self *TestMultiClientGenerator) NextDestintationIds(count int, excludedClientIds []Id) (map[Id]ByteCount, error) {
	return self.nextDestintationIds(count, excludedClientIds)
}

func (self *TestMultiClientGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	return self.newClientArgs()
}

func (self *TestMultiClientGenerator) RemoveClientArgs(args *MultiClientGeneratorClientArgs) {
	self.removeClientArgs(args)
}

func (self *TestMultiClientGenerator) NewClient(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
	return self.newClient(ctx, args, clientSettings)
}


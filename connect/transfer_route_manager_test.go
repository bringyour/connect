package connect

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/go-playground/assert/v2"
)

func TestMultiRoute(t *testing.T) {
	// create route manager
	// add multiple transports and routes
	// multi route write, write a message
	// multi route reader, read a message

	WriteTimeout := 1 * time.Second
	ReadTimeout := 1 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientId := NewId()
	// client := NewClientWithDefaults(ctx, clientId)

	routeManager := NewRouteManager(ctx, "test")

	sendTransports := map[Transport][]Route{}
	receiveTransports := map[Transport][]Route{}

	transportCount := 20
	burstSize := 2048

	multiRouteWriter := routeManager.OpenMultiRouteWriter(DestinationId(clientId))

	multiRouteReader := routeManager.OpenMultiRouteReader(DestinationId(clientId))

	for i := 0; i < transportCount; i += 1 {
		r := make(chan []byte)
		sendRoutes := []Route{r}
		sendTransport := NewSendGatewayTransport()
		receiveRoutes := []Route{r}
		receiveTransport := NewReceiveGatewayTransport()

		sendTransports[sendTransport] = sendRoutes
		receiveTransports[receiveTransport] = receiveRoutes
	}

	go func() {
		for sendTransport, sendRoutes := range sendTransports {
			routeManager.UpdateTransport(sendTransport, sendRoutes)
		}
		for receiveTransport, receiveRoutes := range receiveTransports {
			routeManager.UpdateTransport(receiveTransport, receiveRoutes)
		}
	}()

	messageBytes := func(i int) []byte {
		b := new(bytes.Buffer)
		err := binary.Write(b, binary.LittleEndian, int64(i))
		if err != nil {
			panic(err)
		}
		return b.Bytes()
	}

	go func() {
		for i := 0; i < burstSize; i += 1 {
			multiRouteWriter.Write(ctx, messageBytes(i), WriteTimeout)
		}
	}()

	messages := [][]byte{}

	for i := 0; i < burstSize; i += 1 {
		b, err := multiRouteReader.Read(ctx, ReadTimeout)
		assert.Equal(t, err, nil)
		// assert.Equal(t, messageBytes(i), b)
		messages = append(messages, b)
	}

	assert.Equal(t, burstSize, len(messages))

	littleEndianCmp := func(a []byte, b []byte) int {
		if len(a) < len(b) {
			return -1
		} else if len(b) < len(a) {
			return 1
		}

		for i := len(a) - 1; 0 <= i; i -= 1 {
			aValue := a[i]
			bValue := b[i]
			if aValue < bValue {
				return -1
			} else if bValue < aValue {
				return 1
			}
		}

		return 0
	}
	slices.SortStableFunc(messages, littleEndianCmp)
	for i := 0; i < burstSize; i += 1 {
		assert.Equal(t, messageBytes(i), messages[i])
	}

	for sendTransport, _ := range sendTransports {
		routeManager.RemoveTransport(sendTransport)
	}
	for receiveTransport, _ := range receiveTransports {
		routeManager.RemoveTransport(receiveTransport)
	}
}

package connect

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestControlSync(t *testing.T) {
	// control sync to flood control messages,
	// drop transports for longer than ack timeout

	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timeout := 60 * time.Second
	dropTimeout := 5 * time.Second
	allowTimeout := 2 * time.Second
	ackTimeout := 100 * time.Millisecond
	sendDelay := 20 * time.Millisecond

	k := 4
	b := 1000

	clientASettings := DefaultClientSettings()
	clientASettings.SendBufferSettings.AckTimeout = ackTimeout
	clientA := NewClient(ctx, NewId(), NewNoContractClientOob(), clientASettings)

	controlClientA := NewClientWithDefaults(ctx, ControlId, NewNoContractClientOob())
	controlClientA.ContractManager().AddNoContractPeer(clientA.ClientId())

	controlSyncM1 := NewControlSync(ctx, clientA, "m1")

	receiveMessageIndexes := make(chan uint32, k*b)
	receiveErrors := make(chan error, 1)
	var receiveDropCount atomic.Int64

	// Snapshot each received test index without blocking the shared callback.
	controlClientA.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			m, err := FromFrame(frame)
			if err != nil {
				select {
				case receiveErrors <- err:
				default:
				}
				continue
			}
			switch v := m.(type) {
			case *protocol.SimpleMessage:
				select {
				case receiveMessageIndexes <- v.MessageIndex:
				default:
					receiveDropCount.Add(1)
				}
			}
		}
	})

	for i := range k {
		go func() {
			for j := range b {
				frame, err := ToFrame(&protocol.SimpleMessage{
					MessageIndex: uint32(i*b + j),
				}, DefaultProtocolVersion)
				AssertEqual(t, err, nil)
				controlSyncM1.Send(
					frame,
					nil,
					nil,
				)
				select {
				case <-time.After(time.Duration(mathrand.Int63n(int64(sendDelay)))):
				case <-ctx.Done():
					return
				}
			}
		}()

		go func() {
			for {
				// wait
				// create transport
				// wait
				// drop transport

				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(mathrand.Int63n(int64(dropTimeout)))):
				}

				clientASendTransport := NewSendGatewayTransport()
				clientAReceiveTransport := NewReceiveGatewayTransport()

				controlClientASendTransport := NewSendGatewayTransport()
				controlClientAReceiveTransport := NewReceiveGatewayTransport()

				clientASend := make(chan []byte)
				clientAReceive := make(chan []byte)

				clientA.RouteManager().UpdateTransport(clientASendTransport, []Route{clientASend})
				clientA.RouteManager().UpdateTransport(clientAReceiveTransport, []Route{clientAReceive})

				controlClientA.RouteManager().UpdateTransport(controlClientASendTransport, []Route{clientAReceive})
				controlClientA.RouteManager().UpdateTransport(controlClientAReceiveTransport, []Route{clientASend})

				select {
				case <-ctx.Done():
					clientA.RouteManager().UpdateTransport(clientASendTransport, nil)
					clientA.RouteManager().UpdateTransport(clientAReceiveTransport, nil)

					controlClientA.RouteManager().UpdateTransport(controlClientASendTransport, nil)
					controlClientA.RouteManager().UpdateTransport(controlClientAReceiveTransport, nil)
					return
				case <-time.After(time.Duration(mathrand.Int63n(int64(allowTimeout)))):
				}

				clientA.RouteManager().UpdateTransport(clientASendTransport, nil)
				clientA.RouteManager().UpdateTransport(clientAReceiveTransport, nil)

				controlClientA.RouteManager().UpdateTransport(controlClientASendTransport, nil)
				controlClientA.RouteManager().UpdateTransport(controlClientAReceiveTransport, nil)
			}
		}()

		func() {
			p := uint32(0)
			for {
				// select from message channel
				// when select message k + b, stop
				// if timeout, error

				select {
				case messageIndex := <-receiveMessageIndexes:
					end := uint32(b*i + b - 1)
					fmt.Printf("[csync]%d/%d (%d)\n", messageIndex, end, p)

					AssertEqual(t, p <= messageIndex, true)
					p = messageIndex
					if messageIndex == end {
						return
					}
				case err := <-receiveErrors:
					t.Fatalf("decode control message: %v", err)
				case <-time.After(timeout):
					t.FailNow()
				}
			}
		}()
	}
	if dropCount := receiveDropCount.Load(); dropCount != 0 {
		t.Fatalf("control receive callback dropped %d message(s)", dropCount)
	}
}

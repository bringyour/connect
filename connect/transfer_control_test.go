package connect

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"testing"
	"time"

	"github.com/go-playground/assert/v2"

	"bringyour.com/protocol"
)

func TestControlSync(t *testing.T) {
	// control sync to flood control messages,
	// drop transports for longer than ack timeout

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

	receive := make(chan *protocol.SimpleMessage)

	// on receive a test message, set the channel
	controlClientA.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
		for _, frame := range frames {
			m, err := FromFrame(frame)
			assert.Equal(t, err, nil)
			switch v := m.(type) {
			case *protocol.SimpleMessage:
				select {
				case <-ctx.Done():
				case receive <- v:
				case <-time.After(timeout):
					t.FailNow()
				}
			}
		}
	})

	for i := range k {
		go func() {
			for j := range b {
				frame, err := ToFrame(&protocol.SimpleMessage{
					MessageIndex: uint32(i*b + j),
				})
				assert.Equal(t, err, nil)
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
				case m := <-receive:
					end := uint32(b*i + b - 1)
					fmt.Printf("[csync]%d/%d (%d)\n", m.MessageIndex, end, p)

					assert.Equal(t, p <= m.MessageIndex, true)
					p = m.MessageIndex
					if m.MessageIndex == end {
						return
					}
				case <-time.After(timeout):
					t.FailNow()
				}
			}
		}()
	}
}

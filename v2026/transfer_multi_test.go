package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestSendMultiWithTimeoutDeliversOneBatchAndOneAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	sender := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer sender.Cancel()

	receiverSettings := DefaultClientSettings()
	receiverSettings.EncryptionSettings.Mode = EncryptionModeOff
	receiver := NewClient(ctx, NewId(), NewNoContractClientOob(), receiverSettings)
	defer receiver.Cancel()

	toReceiver := make(chan []byte, 8)
	toSender := make(chan []byte, 8)
	sender.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(receiver.ClientId())),
		[]Route{toReceiver},
	)
	receiver.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{toReceiver})
	receiver.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(sender.ClientId())),
		[]Route{toSender},
	)
	sender.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{toSender})
	sender.ContractManager().AddNoContractPeer(receiver.ClientId())
	receiver.ContractManager().AddNoContractPeer(sender.ClientId())

	received := make(chan []string, 1)
	var callbackCount atomic.Int32
	var callbackDropCount atomic.Int32
	unsub := receiver.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		callbackCount.Add(1)
		contents := make([]string, 0, len(frames))
		for _, frame := range frames {
			message, err := FromFrame(frame)
			if err != nil {
				t.Errorf("decode frame: %v", err)
				return
			}
			simple, ok := message.(*protocol.SimpleMessage)
			if !ok {
				t.Errorf("message type = %T, want *protocol.SimpleMessage", message)
				return
			}
			contents = append(contents, simple.Content)
		}
		select {
		case received <- contents:
		default:
			callbackDropCount.Add(1)
		}
	})
	defer unsub()

	frames := []*protocol.Frame{
		RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: "first"}),
		RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: "second"}),
	}
	ack := make(chan error, 1)
	var ackCount atomic.Int32
	if !sender.SendMultiWithTimeout(
		frames,
		receiver.ClientId(),
		func(err error) {
			ackCount.Add(1)
			ack <- err
		},
		5*time.Second,
	) {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		t.Fatal("batch send was not accepted")
	}

	select {
	case contents := <-received:
		if len(contents) != 2 || contents[0] != "first" || contents[1] != "second" {
			t.Fatalf("received batch = %v", contents)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for batch")
	}
	if dropCount := callbackDropCount.Load(); dropCount != 0 {
		t.Fatalf("receive callback dropped %d batch(es)", dropCount)
	}
	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("batch ack: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for batch ack")
	}
	if got := callbackCount.Load(); got != 1 {
		t.Fatalf("receive callback count = %d, want 1", got)
	}
	if got := ackCount.Load(); got != 1 {
		t.Fatalf("ack callback count = %d, want 1", got)
	}
}

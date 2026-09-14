package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestRemoteUserNatProviderPingOwnsAsyncEcho pins the callback-lifetime
// boundary at the provider ping echo. Receive frames are pooled and borrowed;
// an asynchronous send must own a distinct Frame and retain the message bytes.
//
// The production failure charged the empty IpPing's one-byte contract minimum,
// then the receive-frame pool reused that same Frame for a 49/88-byte message
// before serialization. Its later ACK panicked sequence accounting and stalled
// every packet behind that send sequence.
func TestRemoteUserNatProviderPingOwnsAsyncEcho(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.ControlPingTimeout = 0
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	peerId := NewId()
	transferKey := TransferKey{
		EncryptionRole: protocol.SequenceRole_SequenceRoleServer,
	}
	id := sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
		EncryptionRole:    sequenceTlsRoleServer,
	}
	providerSettings := DefaultRemoteUserNatProviderSettings()
	provider := &RemoteUserNatProvider{
		ctx:                      ctx,
		client:                   client,
		settings:                 providerSettings,
		sourceProvideMode:        map[Id]protocol.ProvideMode{},
		sourceP2pPriorityRefresh: map[Id]time.Time{},
	}

	// Install a deliberately paused sequence. This makes the asynchronous
	// ownership boundary deterministic: ClientReceive can enqueue the echo,
	// but no sequence goroutine serializes it before we recycle the input.
	sequence := installProviderReturnTestSequence(t, provider, client, id)

	want := []byte{0x70, 0x69, 0x6e, 0x67}
	inboundBytes := MessagePoolCopy(want)
	inbound := &protocol.Frame{
		MessageType:  protocol.MessageType_IpIpPing,
		MessageBytes: inboundBytes,
		Raw:          true,
	}

	provider.ClientReceive(
		SourceId(peerId),
		[]*protocol.Frame{inbound},
		Peer{
			ProvideMode: protocol.ProvideMode_Public,
			TransferKey: transferKey,
		},
	)

	var queued *SendPack
	select {
	case queued = <-sequence.packs:
	default:
		t.Fatal("provider did not enqueue ping echo")
	}
	if queued.Frame == inbound {
		t.Fatal("ping echo retained the borrowed receive Frame")
	}
	if queued.Destination != peerId {
		t.Fatalf("ping echo destination = %s, want %s", queued.Destination, peerId)
	}

	// Simulate the receive owner returning and resetting its decoded Frame as
	// soon as the callback completes.
	MessagePoolReturn(inbound.MessageBytes)
	inbound.MessageType = protocol.MessageType_IpIpPacketFromProvider
	inbound.MessageBytes = []byte("reused receive frame")
	inbound.Raw = false

	if queued.Frame.MessageType != protocol.MessageType_IpIpPing {
		t.Fatalf("queued echo changed after input reuse: %v", queued.Frame.MessageType)
	}
	if !queued.Frame.Raw {
		t.Fatal("queued echo lost raw encoding after input reuse")
	}
	if string(queued.Frame.MessageBytes) != string(want) {
		t.Fatalf("queued echo bytes changed after input reuse: got %q want %q", queued.Frame.MessageBytes, want)
	}
	MessagePoolReturn(queued.Frame.MessageBytes)
}

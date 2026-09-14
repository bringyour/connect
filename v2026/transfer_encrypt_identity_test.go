package connect

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestPeerIdentitiesEstablishedAndVerified pins the peer-identity surface the
// post quantum identity panel consumes: once a per-peer session establishes
// and the peer identity proof verifies, `PeerIdentities` on each side reports
// the peer with its long-lived public identity key, and the change callback
// fires. Mirrors the TestSendReceiveEncryptedWithContracts harness. Runs in
// both handshake-control carrier modes: symmetric EncryptedControl and the
// production-default companion-contract carrier.
func TestPeerIdentitiesEstablishedAndVerified(t *testing.T) {
	runPeerIdentitiesEstablishedAndVerified(t, false)
}

func TestPeerIdentitiesEstablishedAndVerifiedCompanionControl(t *testing.T) {
	runPeerIdentitiesEstablishedAndVerified(t, true)
}

func runPeerIdentitiesEstablishedAndVerified(t *testing.T, encryptionControlUseCompanion bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aClientId := NewId()
	bClientId := NewId()

	aSend := make(chan []byte)
	bSend := make(chan []byte)

	aConditioner, bReceive := newConditioner(ctx, aSend)
	bConditioner, aReceive := newConditioner(ctx, bSend)
	aConditioner.update(func() { aConditioner.randomDelay = 20 * time.Millisecond })
	bConditioner.update(func() { bConditioner.randomDelay = 20 * time.Millisecond })

	aSendTransport := newDataGatewayTransport()
	aReceiveTransport := NewReceiveGatewayTransport()
	bSendTransport := newDataGatewayTransport()
	bReceiveTransport := NewReceiveGatewayTransport()

	provideModes := map[protocol.ProvideMode]bool{protocol.ProvideMode_Network: true}

	makeSettings := func() *ClientSettings {
		s := DefaultClientSettings()
		s.SendBufferSettings.SequenceBufferSize = 0
		s.SendBufferSettings.AckBufferSize = 0
		s.SendBufferSettings.AckTimeout = 60 * time.Second
		s.SendBufferSettings.IdleTimeout = 1 * time.Second
		s.SendBufferSettings.MinResendInterval = 10 * time.Millisecond
		s.ReceiveBufferSettings.SequenceBufferSize = 0
		s.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		s.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		s.ForwardBufferSettings.SequenceBufferSize = 0
		s.ForwardBufferSettings.IdleTimeout = 1 * time.Second
		s.ContractManagerSettings.LegacyCreateContract = false
		s.EncryptionSettings.Mode = EncryptionModeOpportunistic
		s.EncryptionSettings.TlsTimeout = 30 * time.Second
		s.EncryptionSettings.EncryptionControlUseCompanion = encryptionControlUseCompanion
		return s
	}

	var a, b *Client
	aOob := &grantingClientOob{
		sourceId: aClientId,
		settings: DefaultContractManagerSettings(),
		destSecretKey: func(destinationId Id) ([]byte, bool) {
			return b.ContractManager().GetProvideSecretKey(protocol.ProvideMode_Network)
		},
		destClientPublicKey: func(destinationId Id) []byte {
			return b.ClientKeyManager().PublicKey()
		},
	}
	bOob := &grantingClientOob{
		sourceId: bClientId,
		settings: DefaultContractManagerSettings(),
		destSecretKey: func(destinationId Id) ([]byte, bool) {
			return a.ContractManager().GetProvideSecretKey(protocol.ProvideMode_Network)
		},
		destClientPublicKey: func(destinationId Id) []byte {
			return a.ClientKeyManager().PublicKey()
		},
	}

	a = NewClient(ctx, aClientId, aOob, makeSettings())
	defer a.Cancel()
	a.RouteManager().UpdateTransport(aSendTransport, []Route{aSend})
	a.RouteManager().UpdateTransport(aReceiveTransport, []Route{aReceive})
	blackholeControlId(ctx, a.RouteManager())
	a.ContractManager().SetProvideModes(provideModes)

	b = NewClient(ctx, bClientId, bOob, makeSettings())
	defer b.Cancel()
	b.RouteManager().UpdateTransport(bSendTransport, []Route{bSend})
	b.RouteManager().UpdateTransport(bReceiveTransport, []Route{bReceive})
	blackholeControlId(ctx, b.RouteManager())
	b.ContractManager().SetProvideModes(provideModes)

	// the change callback fires when the established + verified set changes
	var aChanges, bChanges int64
	unsubA := a.EncryptionSessionManager().AddPeerIdentityChangeCallback(func() {
		atomic.AddInt64(&aChanges, 1)
	})
	defer unsubA()
	unsubB := b.EncryptionSessionManager().AddPeerIdentityChangeCallback(func() {
		atomic.AddInt64(&bChanges, 1)
	})
	defer unsubB()

	// before any traffic, both sets are empty
	AssertEqual(t, 0, len(a.EncryptionSessionManager().PeerIdentities()))
	AssertEqual(t, 0, len(b.EncryptionSessionManager().PeerIdentities()))

	send := func(client *Client, dst Id, label string) {
		m := &protocol.SimpleMessage{Content: label}
		frame, err := ToFrame(m, DefaultProtocolVersion)
		if err != nil {
			panic(err)
		}
		client.Send(frame, dst, func(error) {})
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for i := 0; ; i += 1 {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			default:
			}
			send(a, bClientId, fmt.Sprintf("a%d", i))
			send(b, aClientId, fmt.Sprintf("b%d", i))
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()

	// poll until each side reports the peer's verified identity
	identityReported := func(client *Client, peerId Id, peerPublicKey []byte) bool {
		for _, peerIdentity := range client.EncryptionSessionManager().PeerIdentities() {
			if peerIdentity.PeerId == peerId {
				return string(peerIdentity.PublicKey) == string(peerPublicKey)
			}
		}
		return false
	}
	endTime := time.Now().Add(45 * time.Second)
	for {
		aReports := identityReported(a, bClientId, b.ClientKeyManager().PublicKey())
		bReports := identityReported(b, aClientId, a.ClientKeyManager().PublicKey())
		if aReports && bReports {
			break
		}
		if endTime.Before(time.Now()) {
			t.Fatalf(
				"peer identities not reported before deadline: a=%t b=%t",
				aReports, bReports,
			)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ctx done before identities reported")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// the change callbacks fired on the way to the verified state
	AssertEqual(t, true, 0 < atomic.LoadInt64(&aChanges))
	AssertEqual(t, true, 0 < atomic.LoadInt64(&bChanges))
}

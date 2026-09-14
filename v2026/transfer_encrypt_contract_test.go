package connect

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// grantingClientOob is a test ClientOob that answers CreateContract requests
// by issuing a signed contract from sourceId to the requested destination,
// using the destination's provide secret key (so the contract verifies on the
// receiver). Unlike NewNoContractClientOob (which fails CreateContract), this
// exercises the real contract-negotiation path — required to reproduce
// contract-coupled encryption behavior, e.g. the per-peer EncryptedControl
// carrier negotiating its own contracts alongside normal application data.
type grantingClientOob struct {
	sourceId Id
	settings *ContractManagerSettings
	// destSecretKey returns the provide secret key the destination uses to
	// sign contracts addressed to it. Looked up lazily so it can reference a
	// peer client created after this oob.
	destSecretKey func(destinationId Id) ([]byte, bool)
	// destClientPublicKey returns the destination's long-lived Ed25519 client
	// public key, sealed into the contract so the sender can verify the
	// destination's per-peer-session identity proof (establishing the cipher).
	destClientPublicKey func(destinationId Id) []byte
}

func (self *grantingClientOob) SendControl(frames []*protocol.Frame, callback func([]*protocol.Frame, error)) {
	var out []*protocol.Frame
	for _, frame := range frames {
		message, err := FromFrame(frame)
		if err != nil {
			continue
		}
		createContract, ok := message.(*protocol.CreateContract)
		if !ok {
			continue
		}
		destinationId, err := IdFromBytes(createContract.DestinationId)
		if err != nil {
			continue
		}
		secretKey, ok := self.destSecretKey(destinationId)
		if !ok {
			continue
		}
		storedContract := &protocol.StoredContract{
			ContractId:        NewId().Bytes(),
			TransferByteCount: createContract.TransferByteCount,
			SourceId:          self.sourceId.Bytes(),
			DestinationId:     destinationId.Bytes(),
		}
		if self.destClientPublicKey != nil {
			storedContract.DestinationClientPublicKey = self.destClientPublicKey(destinationId)
		}
		storedContractBytes, err := ProtoMarshal(storedContract)
		if err != nil {
			continue
		}
		hmac := SignStoredContract(self.settings, secretKey, storedContractBytes)
		result := &protocol.CreateContractResult{
			Contract: &protocol.Contract{
				StoredContractBytes: storedContractBytes,
				StoredContractHmac:  hmac,
				ProvideMode:         protocol.ProvideMode_Network,
			},
		}
		resultFrame, err := ToFrame(result, DefaultProtocolVersion)
		if err != nil {
			continue
		}
		out = append(out, resultFrame)
	}
	if callback != nil {
		HandleError(func() {
			callback(out, nil)
		})
	}
}

// dataGatewayTransport is a send gateway that matches every destination
// except ControlId. Control-plane frames (addressed to ControlId) are routed
// to controlBlackholeTransport instead, so they are never forwarded
// peer-to-peer by a catch-all gateway (which, with no platform to terminate
// them, would loop forever).
type dataGatewayTransport struct {
	transportId Id
}

func newDataGatewayTransport() *dataGatewayTransport {
	return &dataGatewayTransport{transportId: NewId()}
}

func (self *dataGatewayTransport) TransportId() Id { return self.transportId }
func (self *dataGatewayTransport) Priority() int   { return 100 }
func (self *dataGatewayTransport) Weight() float32 { return 0 }
func (self *dataGatewayTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}
func (self *dataGatewayTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	return 1.0 / float32(1+len(remainingStats))
}
func (self *dataGatewayTransport) MatchesSend(destination TransferPath) bool {
	return destination.DestinationId != ControlId
}
func (self *dataGatewayTransport) MatchesReceive(destination TransferPath) bool { return false }
func (self *dataGatewayTransport) Downgrade(source TransferPath)                {}

// controlBlackholeTransport sinks send traffic addressed to ControlId. The
// two-client test harness has no platform to terminate control-plane frames
// (e.g. the EncryptedKey publish, which correctly retries via ControlSync), so
// without a sink those frames would be forwarded peer-to-peer forever by the
// catch-all gateway transports. Routing them to a drained channel terminates
// them deterministically — no reliance on packet loss to break the loop.
type controlBlackholeTransport struct {
	transportId Id
}

func newControlBlackholeTransport() *controlBlackholeTransport {
	return &controlBlackholeTransport{transportId: NewId()}
}

func (self *controlBlackholeTransport) TransportId() Id { return self.transportId }
func (self *controlBlackholeTransport) Priority() int   { return TransportMaxPriority }
func (self *controlBlackholeTransport) Weight() float32 { return 0 }
func (self *controlBlackholeTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}
func (self *controlBlackholeTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	return 1.0
}
func (self *controlBlackholeTransport) MatchesSend(destination TransferPath) bool {
	return destination.DestinationId == ControlId
}
func (self *controlBlackholeTransport) MatchesReceive(destination TransferPath) bool { return false }
func (self *controlBlackholeTransport) Downgrade(source TransferPath)                {}

// blackholeControlId registers a controlBlackholeTransport on routeManager and
// drains it, sinking all ControlId-addressed traffic for the lifetime of ctx.
func blackholeControlId(ctx context.Context, routeManager *RouteManager) {
	transport := newControlBlackholeTransport()
	sink := make(chan []byte)
	routeManager.UpdateTransport(transport, []Route{sink})
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sink:
			}
		}
	}()
}

// Counts application frames inline without handing receive-pump work to a
// queue that can fill or block.
func countSimpleMessageFrames(frames []*protocol.Frame, receiveCount *int64) {
	for _, frame := range frames {
		if message, err := FromFrame(frame); err == nil {
			if _, ok := message.(*protocol.SimpleMessage); ok {
				atomic.AddInt64(receiveCount, 1)
			}
		}
	}
}

// TestSendReceiveEncryptedWithContracts exercises bidirectional encrypted
// transfer when contracts are required and supplied on demand by a
// contract-granting oob. The send idle timeout is short so send sequences
// exit and flush their contract queue between bursts — the condition that, in
// the integration test, starved the EncryptedControl carrier of contracts
// when both per-peer encryption send sequences shared one ContractKey. With
// per-role ContractKeys the two sequences own separate queues, so one's
// exit-flush no longer discards the other's contracts and the handshake
// completes.
func TestSendReceiveEncryptedWithContracts(t *testing.T) {
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

	// Data gateways match every destination except ControlId; ControlId
	// traffic is sunk by a blackhole (see blackholeControlId below) so the
	// EncryptedKey publish doesn't loop peer-to-peer in this platform-less
	// harness.
	aSendTransport := newDataGatewayTransport()
	aReceiveTransport := NewReceiveGatewayTransport()
	bSendTransport := newDataGatewayTransport()
	bReceiveTransport := NewReceiveGatewayTransport()

	provideModes := map[protocol.ProvideMode]bool{protocol.ProvideMode_Network: true}

	makeSettings := func() *ClientSettings {
		s := DefaultClientSettingsWithBufferSize(64)
		s.SendBufferSettings.AckTimeout = 60 * time.Second
		// Short send idle so a send sequence exits (and flushes its contract
		// queue) between bursts.
		s.SendBufferSettings.IdleTimeout = 1 * time.Second
		s.SendBufferSettings.MinResendInterval = 10 * time.Millisecond
		s.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		s.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		s.ForwardBufferSettings.IdleTimeout = 1 * time.Second
		s.ContractManagerSettings.LegacyCreateContract = false
		s.EncryptionSettings.Mode = EncryptionModeOpportunistic
		s.EncryptionSettings.TlsTimeout = 30 * time.Second
		// Exercise the symmetric (non-companion) EncryptedControl path.
		s.EncryptionSettings.EncryptionControlUseCompanion = false
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

	var gotA, gotB int64
	a.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, _ Peer) {
		countSimpleMessageFrames(frames, &gotA)
	})
	b.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, _ Peer) {
		countSimpleMessageFrames(frames, &gotB)
	})

	send := func(client *Client, dst Id, label string) {
		m := &protocol.SimpleMessage{Content: label}
		frame, err := ToFrame(m, DefaultProtocolVersion)
		if err != nil {
			panic(err)
		}
		client.Send(frame, dst, func(error) {})
	}

	// Drive both directions in parallel: A and B each initiate to the other at
	// the same time, so each client holds four sequences for the peer —
	// send/receive on its client role (its own outbound handshake) and
	// send/receive on its server role (the peer's). Each send reuses its
	// per-role send sequence, so the handshake runs once and completes (rather
	// than restarting per burst) while keeping the per-peer sessions referenced
	// through it. Encryption is confirmed by (a) the per-peer client cipher
	// coming up in both directions and (b) messages continuing to round-trip
	// after that — encrypted messages whose receipt necessarily waited for the
	// handshake.
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
	cipherUp := func(client *Client, peer Id) bool {
		sess := client.EncryptionSessionManager().Lookup(peer, sequenceTlsRoleClient, false)
		return sess != nil && sess.Cipher() != nil
	}

	// Wait for the per-peer client cipher to establish in both directions.
	deadline := time.After(45 * time.Second)
	for !(cipherUp(a, bClientId) && cipherUp(b, aClientId)) {
		select {
		case <-deadline:
			t.Fatalf("encryption did not establish: a->b cipher=%t, b->a cipher=%t (a received %d, b received %d)",
				cipherUp(a, bClientId), cipherUp(b, aClientId), atomic.LoadInt64(&gotA), atomic.LoadInt64(&gotB))
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Both ciphers up: confirm encrypted messages keep round-tripping (sent now,
	// after the cipher, so they are wrapped).
	baseA, baseB := atomic.LoadInt64(&gotA), atomic.LoadInt64(&gotB)
	deadline2 := time.After(20 * time.Second)
	for atomic.LoadInt64(&gotA) < baseA+3 || atomic.LoadInt64(&gotB) < baseB+3 {
		select {
		case <-deadline2:
			t.Fatalf("messages stopped after cipher established: a received %d (want %d), b received %d (want %d)",
				atomic.LoadInt64(&gotA), baseA+3, atomic.LoadInt64(&gotB), baseB+3)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestSendReceiveEncryptedPeerWithoutEncryption pins the opportunistic
// fallback: an initiator with `Encrypt=true` talking to a peer whose session
// manager is inert (`Encrypt=false` — `DeliverEncryptedControl` drops
// handshake controls, acquire/lookup return nil). The initiator's handshake
// can never complete, so its per-peer sessions stay cipher-nil and traffic
// flows in plaintext at this layer, in both directions — the sessions never
// block or wedge the send/receive sequences. This is the compatibility
// contract the per-peer encryption toggle relies on against providers that
// don't support encryption; the both-sides-enabled contrast (cipher comes up,
// traffic continues) is TestSendReceiveEncryptedWithContracts above.
func TestSendReceiveEncryptedPeerWithoutEncryption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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

	makeSettings := func(encrypt bool) *ClientSettings {
		s := DefaultClientSettingsWithBufferSize(64)
		s.SendBufferSettings.AckTimeout = 60 * time.Second
		s.SendBufferSettings.IdleTimeout = 60 * time.Second
		s.SendBufferSettings.MinResendInterval = 10 * time.Millisecond
		s.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		s.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		s.ForwardBufferSettings.IdleTimeout = 1 * time.Second
		s.ContractManagerSettings.LegacyCreateContract = false
		if encrypt {
			s.EncryptionSettings.Mode = EncryptionModeOpportunistic
		} else {
			s.EncryptionSettings.Mode = EncryptionModeOff
		}
		s.EncryptionSettings.TlsTimeout = 30 * time.Second
		s.EncryptionSettings.EncryptionControlUseCompanion = false
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

	// a initiates with encryption enabled; b is the peer without encryption.
	a = NewClient(ctx, aClientId, aOob, makeSettings(true))
	defer a.Cancel()
	a.RouteManager().UpdateTransport(aSendTransport, []Route{aSend})
	a.RouteManager().UpdateTransport(aReceiveTransport, []Route{aReceive})
	blackholeControlId(ctx, a.RouteManager())
	a.ContractManager().SetProvideModes(provideModes)

	b = NewClient(ctx, bClientId, bOob, makeSettings(false))
	defer b.Cancel()
	b.RouteManager().UpdateTransport(bSendTransport, []Route{bSend})
	b.RouteManager().UpdateTransport(bReceiveTransport, []Route{bReceive})
	blackholeControlId(ctx, b.RouteManager())
	b.ContractManager().SetProvideModes(provideModes)

	// On b the receive sequences hold no session (inert manager), so a's
	// handshake EncryptedControl frames bubble up to the receive callback as
	// ordinary frames; filtering on SimpleMessage ignores them, as an
	// application would ignore frame types it doesn't consume.
	var gotA, gotB int64
	a.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, _ Peer) {
		countSimpleMessageFrames(frames, &gotA)
	})
	b.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, _ Peer) {
		countSimpleMessageFrames(frames, &gotB)
	})

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
	// assertNoCipher sweeps every per-peer session a holds for b — the
	// client-role initiator plus any receive-side sessions — and requires all
	// of them cipher-nil: with b's manager inert the handshake has no
	// responder, so no session may ever expose a cipher.
	assertNoCipher := func(tag string) {
		for _, sess := range a.EncryptionSessionManager().sessionsForPeer(bClientId) {
			if cipher := sess.Cipher(); cipher != nil {
				t.Fatalf("%s: a's session for b exposes a cipher — the handshake cannot establish against an Encrypt=false peer", tag)
			}
		}
	}

	// Phase 1: application traffic must be delivered in both directions —
	// plaintext at this layer — while a's handshake attempt stays pending.
	deadline := time.After(45 * time.Second)
	for atomic.LoadInt64(&gotA) < 4 || atomic.LoadInt64(&gotB) < 4 {
		assertNoCipher("phase 1")
		select {
		case <-deadline:
			t.Fatalf("plaintext fallback did not deliver: a received %d, b received %d — traffic must flow when the peer has encryption disabled",
				atomic.LoadInt64(&gotA), atomic.LoadInt64(&gotB))
		case <-time.After(50 * time.Millisecond):
		}
	}

	// a initiated, so its client-role session for b exists — proving the
	// fallback path was exercised (a session with a live handshake attempt,
	// not a skipped one) — and stays cipher-nil.
	aClientSession := a.EncryptionSessionManager().Lookup(bClientId, sequenceTlsRoleClient, false)
	if aClientSession == nil {
		t.Fatal("expected a's client-role session for b to exist (a initiates the handshake)")
	}
	AssertEqual(t, (*sequenceCipher)(nil), aClientSession.Cipher())
	assertNoCipher("after delivery")

	// b's manager is inert: no session in any role/companion.
	for _, role := range []sequenceTlsRole{sequenceTlsRoleClient, sequenceTlsRoleServer} {
		for _, companion := range []bool{false, true} {
			if b.EncryptionSessionManager().Lookup(aClientId, role, companion) != nil {
				t.Fatalf("expected no session on the Encrypt=false peer (role=%v companion=%t)", role, companion)
			}
		}
	}

	// Phase 2: flow is sustained — the unestablished session must not wedge
	// the sequences as more traffic moves through them.
	baseA, baseB := atomic.LoadInt64(&gotA), atomic.LoadInt64(&gotB)
	deadline2 := time.After(20 * time.Second)
	for atomic.LoadInt64(&gotA) < baseA+3 || atomic.LoadInt64(&gotB) < baseB+3 {
		select {
		case <-deadline2:
			t.Fatalf("messages stopped flowing: a received %d (want %d), b received %d (want %d)",
				atomic.LoadInt64(&gotA), baseA+3, atomic.LoadInt64(&gotB), baseB+3)
		case <-time.After(50 * time.Millisecond):
		}
	}
	assertNoCipher("after sustained flow")
}

// TestRequiredEncryptionEstablishesAndDelivers is the positive control for
// EncryptionModeRequired: two Required peers must still complete the handshake
// and exchange application data. Because Required holds plaintext application
// data on send and drops it on receive, any SimpleMessage that arrives here was
// necessarily wrapped — delivery alone proves encryption. This pins that the
// fail-closed gates do not break the happy path (handshake control frames are
// exempt, so the cipher still bootstraps).
func TestRequiredEncryptionEstablishesAndDelivers(t *testing.T) {
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
		s := DefaultClientSettingsWithBufferSize(64)
		s.SendBufferSettings.AckTimeout = 60 * time.Second
		s.SendBufferSettings.IdleTimeout = 1 * time.Second
		s.SendBufferSettings.MinResendInterval = 10 * time.Millisecond
		s.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		s.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		s.ForwardBufferSettings.IdleTimeout = 1 * time.Second
		s.ContractManagerSettings.LegacyCreateContract = false
		s.EncryptionSettings.Mode = EncryptionModeRequired
		s.EncryptionSettings.TlsTimeout = 30 * time.Second
		// The production default: the server-role EC reply carrier rides a
		// companion contract. The client-role ClientHello always shares the
		// application data sequence either way — the Required entry gate is
		// what keeps held application packs from taking sequence numbers ahead
		// of it.
		s.EncryptionSettings.EncryptionControlUseCompanion = true
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

	aEvents := make(chan *EncryptionEvent, 64)
	aEventsUnsub := a.EncryptionSessionManager().AddEncryptionEventCallback(func(event *EncryptionEvent) {
		select {
		case aEvents <- event:
		default:
		}
	})
	defer aEventsUnsub()

	var gotA, gotB int64
	a.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, _ Peer) {
		countSimpleMessageFrames(frames, &gotA)
	})
	b.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, _ Peer) {
		countSimpleMessageFrames(frames, &gotB)
	})

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
	cipherUp := func(client *Client, peer Id) bool {
		sess := client.EncryptionSessionManager().Lookup(peer, sequenceTlsRoleClient, false)
		return sess != nil && sess.Cipher() != nil
	}

	deadline := time.After(45 * time.Second)
	for !(cipherUp(a, bClientId) && cipherUp(b, aClientId)) {
		select {
		case <-deadline:
			t.Fatalf("encryption did not establish under Required: a->b cipher=%t, b->a cipher=%t (a received %d, b received %d)",
				cipherUp(a, bClientId), cipherUp(b, aClientId), atomic.LoadInt64(&gotA), atomic.LoadInt64(&gotB))
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Cipher up: confirm messages round-trip. Every delivered message is wrapped
	// (Required refuses plaintext application data at both ends).
	baseA, baseB := atomic.LoadInt64(&gotA), atomic.LoadInt64(&gotB)
	deadline2 := time.After(20 * time.Second)
	for atomic.LoadInt64(&gotA) < baseA+3 || atomic.LoadInt64(&gotB) < baseB+3 {
		select {
		case <-deadline2:
			t.Fatalf("encrypted messages did not flow under Required: a received %d (want %d), b received %d (want %d)",
				atomic.LoadInt64(&gotA), baseA+3, atomic.LoadInt64(&gotB), baseB+3)
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Observability: both ends report a sealed state carrying the negotiated
	// post-quantum group (both sides prefer X25519MLKEM768, so the hybrid is
	// the deterministic outcome — this also regression-pins the PQ key
	// exchange itself), and a Sealed event fired for the peer.
	assertSealedState := func(client *Client, peer Id) {
		for _, state := range client.EncryptionSessionManager().PeerEncryptionStates() {
			if state.PeerId == peer {
				AssertEqual(t, true, state.Sealed)
				AssertEqual(t, tls.X25519MLKEM768, state.KeyExchange)
				return
			}
		}
		t.Fatalf("no encryption state for peer %s", peer)
	}
	assertSealedState(a, bClientId)
	assertSealedState(b, aClientId)

	sealedDeadline := time.After(5 * time.Second)
	for {
		sealedSeen := false
		select {
		case event := <-aEvents:
			if event.Type == EncryptionEventSealed && event.PeerId == bClientId {
				sealedSeen = true
			}
		case <-sealedDeadline:
			t.Fatal("no Sealed event for the peer despite an established cipher")
		}
		if sealedSeen {
			break
		}
	}
}

// TestRequiredEncryptionFailsClosedAgainstPlaintextPeer is the core security
// property of EncryptionModeRequired: no application data crosses to or from a
// peer that cannot establish a session. Here a runs Required and b runs Off
// (its session manager is inert, so a's handshake can never complete):
//
//   - a -> b: a holds every application frame on send (the cipher never comes
//     up) and never falls back to plaintext, so b receives nothing.
//   - b -> a: b sends plaintext application packs, which a's receive gate drops
//     because a expects a session for b, so a receives nothing.
//
// Handshake control frames are still exchanged (ForceUnwrapped), so a's
// client-role session for b exists but stays cipher-nil. Contrast with
// TestSendReceiveEncryptedPeerWithoutEncryption, where the same topology under
// Opportunistic delivers plaintext both ways.
func TestRequiredEncryptionFailsClosedAgainstPlaintextPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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

	makeSettings := func(mode EncryptionMode) *ClientSettings {
		s := DefaultClientSettingsWithBufferSize(64)
		s.SendBufferSettings.AckTimeout = 60 * time.Second
		s.SendBufferSettings.IdleTimeout = 60 * time.Second
		s.SendBufferSettings.MinResendInterval = 10 * time.Millisecond
		s.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		s.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		s.ForwardBufferSettings.IdleTimeout = 1 * time.Second
		s.ContractManagerSettings.LegacyCreateContract = false
		s.EncryptionSettings.Mode = mode
		// short establishment bound so the parked send's blocked event (fired
		// when a wait crosses TlsTimeout) lands inside the watch window
		s.EncryptionSettings.TlsTimeout = 5 * time.Second
		s.EncryptionSettings.EncryptionControlUseCompanion = true
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

	// a requires encryption; b has it disabled (cannot establish a session).
	a = NewClient(ctx, aClientId, aOob, makeSettings(EncryptionModeRequired))
	defer a.Cancel()
	a.RouteManager().UpdateTransport(aSendTransport, []Route{aSend})
	a.RouteManager().UpdateTransport(aReceiveTransport, []Route{aReceive})
	blackholeControlId(ctx, a.RouteManager())
	a.ContractManager().SetProvideModes(provideModes)

	aEvents := make(chan *EncryptionEvent, 64)
	aEventsUnsub := a.EncryptionSessionManager().AddEncryptionEventCallback(func(event *EncryptionEvent) {
		select {
		case aEvents <- event:
		default:
		}
	})
	defer aEventsUnsub()

	b = NewClient(ctx, bClientId, bOob, makeSettings(EncryptionModeOff))
	defer b.Cancel()
	b.RouteManager().UpdateTransport(bSendTransport, []Route{bSend})
	b.RouteManager().UpdateTransport(bReceiveTransport, []Route{bReceive})
	blackholeControlId(ctx, b.RouteManager())
	b.ContractManager().SetProvideModes(provideModes)

	var gotA, gotB int64
	a.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, _ Peer) {
		countSimpleMessageFrames(frames, &gotA)
	})
	b.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, _ Peer) {
		countSimpleMessageFrames(frames, &gotB)
	})

	send := func(client *Client, dst Id, label string) {
		m := &protocol.SimpleMessage{Content: label}
		frame, err := ToFrame(m, DefaultProtocolVersion)
		if err != nil {
			panic(err)
		}
		client.Send(frame, dst, func(error) {})
	}

	// Separate send loops: a's Send blocks at the Required entry gate (the
	// cipher never establishes against an Off peer, and Send uses an infinite
	// enqueue timeout), so it must not share a goroutine with b's loop — a
	// single parked a-send would otherwise starve the b direction that
	// exercises the receive gate.
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
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
	go func() {
		for i := 0; ; i += 1 {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			default:
			}
			send(b, aClientId, fmt.Sprintf("b%d", i))
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
	// Watch for 12s (far longer than the ~1s plaintext delivery seen under
	// Opportunistic in TestSendReceiveEncryptedPeerWithoutEncryption): neither
	// counter may ever advance.
	watch := time.After(12 * time.Second)
	watching := true
	for watching {
		select {
		case <-watch:
			watching = false
		case <-time.After(100 * time.Millisecond):
			if got := atomic.LoadInt64(&gotA); got != 0 {
				t.Fatalf("fail-closed violated (receive gate): a received %d application message(s) in plaintext from an Off peer", got)
			}
			if got := atomic.LoadInt64(&gotB); got != 0 {
				t.Fatalf("fail-closed violated (send gate): b received %d application message(s) — a must never emit plaintext", got)
			}
		}
	}

	// a initiated, so its client-role session for b exists (the handshake was
	// attempted, not skipped) and stays cipher-nil — proving traffic was held
	// against a real pending session rather than simply not encrypted.
	aClientSession := a.EncryptionSessionManager().Lookup(bClientId, sequenceTlsRoleClient, false)
	if aClientSession == nil {
		t.Fatal("expected a's client-role session for b to exist (a initiates the handshake)")
	}
	AssertEqual(t, (*sequenceCipher)(nil), aClientSession.Cipher())

	// Ack-and-discard must not wedge the Off sender: a acks the plaintext
	// packs it refuses to deliver, so b's resend queue for a drains. (A
	// gap-producing drop would instead pin the queue at its cap forever.)
	drainDeadline := time.After(20 * time.Second)
	for {
		count, byteCount, _ := b.ResendQueueSize(aClientId, MultiHopId{}, false, false)
		if count == 0 && byteCount == 0 {
			break
		}
		select {
		case <-drainDeadline:
			t.Fatalf("ack-and-discard failed: b's resend queue to a still holds %d item(s) / %d byte(s) — a is not acking refused plaintext", count, byteCount)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Observability under fail-closed: exactly one send-blocked event (the
	// parked infinite-timeout send crossed the 5s establishment bound) and
	// exactly one receive-discarded event (b's plaintext application packs) —
	// each deduplicated per session, and the sessions never seal, so a second
	// occurrence would be a dedup regression.
	blocked, discarded := 0, 0
	eventDeadline := time.After(10 * time.Second)
	for blocked < 1 || discarded < 1 {
		select {
		case event := <-aEvents:
			switch event.Type {
			case EncryptionEventRequiredSendBlocked:
				AssertEqual(t, bClientId, event.PeerId)
				blocked += 1
			case EncryptionEventRequiredReceiveDiscarded:
				AssertEqual(t, bClientId, event.PeerId)
				discarded += 1
			}
		case <-eventDeadline:
			t.Fatalf("missing Required events: sendBlocked=%d receiveDiscarded=%d", blocked, discarded)
		}
	}
	for {
		drained := false
		select {
		case event := <-aEvents:
			switch event.Type {
			case EncryptionEventRequiredSendBlocked:
				blocked += 1
			case EncryptionEventRequiredReceiveDiscarded:
				discarded += 1
			}
		default:
			drained = true
		}
		if drained {
			break
		}
	}
	AssertEqual(t, 1, blocked)
	AssertEqual(t, 1, discarded)
}

// TestEncryptedCompanionSessionsCreateSeparateContracts verifies that the
// per-peer encryption session key includes the companion bit: when A and B each
// send to the other in both companion and non-companion mode, the four
// application flows must run over four cleanly separated client sessions, and
// each peer's two server-role EncryptedControl reply carriers (one echoing the
// companion initiator, one the non-companion initiator) must not collapse onto
// a shared session/contract.
//
// Concretely there are eight send sequences:
//
//	A: client(companion=false)  client(companion=true)  server(companion=false)  server(companion=true)
//	B: client(companion=false)  client(companion=true)  server(companion=false)  server(companion=true)
//
// where each client-role sequence carries A/B's own application data + its
// ClientHello, and each server-role sequence is the reply carrier for the
// peer's corresponding flow. Each send sequence owns a distinct ContractKey
// (and therefore opens its own contract), so exactly four distinct contract
// keys appear in each client's local stats — eight total.
//
// Before companion was added to the session/sequence/contract keys, the two
// server-role reply carriers shared a key (same role, same
// EncryptionControlUseCompanion contract) and collapsed to three keys per
// client — this test pins that they stay separated.
func TestEncryptedCompanionSessionsCreateSeparateContracts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
		s := DefaultClientSettingsWithBufferSize(64)
		s.SendBufferSettings.AckTimeout = 60 * time.Second
		// Long send idle so all eight sequences stay alive (and keep their
		// contracts open) through the assertion — this test is about how many
		// distinct sequences/contracts exist, not contract-flush churn.
		s.SendBufferSettings.IdleTimeout = 60 * time.Second
		s.SendBufferSettings.MinResendInterval = 10 * time.Millisecond
		s.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		s.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		s.ForwardBufferSettings.IdleTimeout = 1 * time.Second
		s.ContractManagerSettings.LegacyCreateContract = false
		s.EncryptionSettings.Mode = EncryptionModeOpportunistic
		s.EncryptionSettings.TlsTimeout = 30 * time.Second
		// Server reply carriers ride regular (non-companion) contracts. This is
		// the case that exercises the ContractKey companion split: both server
		// carriers then share Destination/role/CompanionContract and differ only
		// by the session identity companion.
		s.EncryptionSettings.EncryptionControlUseCompanion = false
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

	// send drives one application message; companion=true rides a companion
	// contract (and so a companion-keyed per-peer session).
	send := func(client *Client, dst Id, label string, companion bool) {
		m := &protocol.SimpleMessage{Content: label}
		frame, err := ToFrame(m, DefaultProtocolVersion)
		if err != nil {
			panic(err)
		}
		if companion {
			client.SendWithTimeout(frame, dst, func(error) {}, 1*time.Second, CompanionContract())
		} else {
			client.SendWithTimeout(frame, dst, func(error) {}, 1*time.Second)
		}
	}

	// Drive all four flows continuously so every handshake starts (and its
	// peer's server reply carrier opens a contract), and so all eight sequences
	// stay referenced.
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
			send(a, bClientId, fmt.Sprintf("a%d", i), false)
			send(a, bClientId, fmt.Sprintf("ac%d", i), true)
			send(b, aClientId, fmt.Sprintf("b%d", i), false)
			send(b, aClientId, fmt.Sprintf("bc%d", i), true)
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()

	// distinctContractKeys returns the set of distinct ContractKeys for which
	// this client currently holds an open send contract — one per live send
	// sequence. Control-plane traffic (the EncryptedKey publish to ControlId)
	// is NoContract, so it never appears here.
	distinctContractKeys := func(client *Client) map[ContractKey]bool {
		keys := map[ContractKey]bool{}
		for _, key := range client.ContractManager().LocalStats().ContractOpenKeys {
			keys[key] = true
		}
		return keys
	}

	// Wait until each client has opened its four distinct send-sequence
	// contracts. If the companion sessions collapsed, a client would stall at
	// three (its two server reply carriers sharing one key) and this times out.
	deadline := time.After(45 * time.Second)
	for {
		aKeys := distinctContractKeys(a)
		bKeys := distinctContractKeys(b)
		if 4 <= len(aKeys) && 4 <= len(bKeys) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf(
				"expected 4 distinct send-sequence contracts per client (8 total); got a=%d b=%d — the companion and non-companion server reply carriers collapsed onto a shared contract",
				len(aKeys), len(bKeys),
			)
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Confirm exactly four per client (eight total) with the expected
	// (role, companion) breakdown: for each role, one companion and one
	// non-companion sequence.
	type roleCompanion struct {
		role      sequenceTlsRole
		companion bool
	}
	assertBreakdown := func(client *Client, peer Id, tag string) {
		keys := distinctContractKeys(client)
		if len(keys) != 4 {
			t.Fatalf("%s: expected exactly 4 distinct contract keys, got %d: %+v", tag, len(keys), keys)
		}
		combos := map[roleCompanion]int{}
		for key := range keys {
			if key.Destination.DestinationId != peer {
				t.Fatalf("%s: unexpected contract destination %s (want peer %s)", tag, key.Destination.DestinationId, peer)
			}
			combos[roleCompanion{role: key.EncryptionRole, companion: key.EncryptionCompanion}] += 1
		}
		for _, role := range []sequenceTlsRole{sequenceTlsRoleClient, sequenceTlsRoleServer} {
			for _, companion := range []bool{false, true} {
				rc := roleCompanion{role: role, companion: companion}
				if combos[rc] != 1 {
					t.Fatalf("%s: expected exactly one contract for %v companion=%t, got %d (combos=%v)", tag, role, companion, combos[rc], combos)
				}
			}
		}
	}
	assertBreakdown(a, bClientId, "a")
	assertBreakdown(b, aClientId, "b")
}

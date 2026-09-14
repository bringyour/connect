package connect

// Deterministic pins for encryption establishment WITHOUT contracts — the
// configuration the integration suite runs as contractTestNone, and the gap
// behind two 5/5 establishment failures when EncryptionModeRequired first ran
// there: the peer's identity key is normally learned from stored contracts
// ("nil until a contract has been seen"), so a contract-free client has
// exactly one key source — the out-of-band
// EncryptionSettings.NewPeerClientPublicKeyFetcher (production: the platform
// GetClientKey api). Without it the identity proof buffers forever and
// Required correctly refuses to bring up a cipher — while Opportunistic
// silently falls back to plaintext, which is why the hole was invisible
// until Required ran at integration scale.

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// contractFreeEncryptedPair wires two clients with NO contract machinery
// (NewNoContractClientOob, network provide) and the given encryption modes.
// When wireKeyFetchers is set, each client's session key fetcher late-binds
// to the peer's ClientKeyManager — the contract-free identity source.
func contractFreeEncryptedPair(
	ctx context.Context,
	aMode EncryptionMode,
	bMode EncryptionMode,
	wireKeyFetchers bool,
) (a *Client, b *Client, bClientId Id, receivesB chan string) {
	aClientId := NewId()
	bClientId = NewId()

	aSend := make(chan []byte)
	bSend := make(chan []byte)

	_, bReceive := newConditioner(ctx, aSend)
	_, aReceive := newConditioner(ctx, bSend)

	aSendTransport := NewSendGatewayTransport()
	aReceiveTransport := NewReceiveGatewayTransport()
	bSendTransport := NewSendGatewayTransport()
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
		// the transfer_test contract-free idiom
		s.ContractManagerSettings.LegacyCreateContract = true
		s.EncryptionSettings.Mode = mode
		s.EncryptionSettings.TlsTimeout = 30 * time.Second
		// no contracts means no companion contracts: the EC reply carrier
		// must ride the plain path
		s.EncryptionSettings.EncryptionControlUseCompanion = false
		return s
	}

	settingsA := makeSettings(aMode)
	settingsB := makeSettings(bMode)

	if wireKeyFetchers {
		settingsA.EncryptionSettings.NewPeerClientPublicKeyFetcher = func(peerId Id) func(ctx context.Context) ([]byte, error) {
			return func(ctx context.Context) ([]byte, error) {
				return b.ClientKeyManager().PublicKey(), nil
			}
		}
		settingsB.EncryptionSettings.NewPeerClientPublicKeyFetcher = func(peerId Id) func(ctx context.Context) ([]byte, error) {
			return func(ctx context.Context) ([]byte, error) {
				return a.ClientKeyManager().PublicKey(), nil
			}
		}
	}

	a = NewClient(ctx, aClientId, NewNoContractClientOob(), settingsA)
	a.RouteManager().UpdateTransport(aSendTransport, []Route{aSend})
	a.RouteManager().UpdateTransport(aReceiveTransport, []Route{aReceive})
	a.ContractManager().SetProvideModes(provideModes)

	b = NewClient(ctx, bClientId, NewNoContractClientOob(), settingsB)
	b.RouteManager().UpdateTransport(bSendTransport, []Route{bSend})
	b.RouteManager().UpdateTransport(bReceiveTransport, []Route{bReceive})
	b.ContractManager().SetProvideModes(provideModes)

	// Hand-feed CERTLESS contracts both directions (the transfer_test
	// idiom: NoContractClientOob only removes the out-of-band path; the
	// sequences still require a contract to send). The hand-built contract
	// carries no ProvideTlsCertificate, so it authorizes transfer WITHOUT
	// providing identity — which is the point: the out-of-band key fetcher
	// stays the ONLY identity source, as in the integration battery's
	// contractTestNone configs. The reverse direction feeds the EC reply
	// carrier.
	for range 2 {
		if err := a.ContractManager().HandleControlFrame(
			ContractKey{Destination: DestinationId(bClientId)},
			requireContractResult(
				protocol.ProvideMode_Network,
				b.ContractManager().RequireProvideSecretKey(protocol.ProvideMode_Network),
				aClientId,
				bClientId,
			),
		); err != nil {
			panic(err)
		}
		if err := b.ContractManager().HandleControlFrame(
			ContractKey{Destination: DestinationId(aClientId)},
			requireContractResult(
				protocol.ProvideMode_Network,
				a.ContractManager().RequireProvideSecretKey(protocol.ProvideMode_Network),
				bClientId,
				aClientId,
			),
		); err != nil {
			panic(err)
		}
	}

	receivesB = make(chan string, 1024)
	b.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, _ Peer) {
		for _, frame := range frames {
			if m, err := FromFrame(frame); err == nil {
				if sm, ok := m.(*protocol.SimpleMessage); ok {
					select {
					case receivesB <- sm.Content:
					default:
					}
				}
			}
		}
	})

	return
}

// TestRequiredContractFreeEstablishesViaOobKeyFetcher: with contracts absent,
// the out-of-band key fetcher is the identity source — Required must
// establish through it and deliver sealed. This is the liveness half of the
// contract-free identity story (the integration battery's contractTestNone
// configs run exactly this once the harness wires the fetcher).
func TestRequiredContractFreeEstablishesViaOobKeyFetcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	a, b, bClientId, receivesB := contractFreeEncryptedPair(
		ctx, EncryptionModeRequired, EncryptionModeRequired, true)
	defer a.Cancel()
	defer b.Cancel()

	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "sealed-contract-free"),
		bClientId,
		func(error) {},
		30*time.Second,
	); !ok {
		t.Fatal("a Required contract-free send with a wired key fetcher was refused: establishment must succeed through the out-of-band key source")
	}
	select {
	case got := <-receivesB:
		AssertEqual(t, "sealed-contract-free", got)
	case <-time.After(30 * time.Second):
		t.Fatal("no delivery: contract-free Required establishment via the key fetcher failed")
	}
	waitForSealedSession(t, ctx, a)
}

// TestRequiredContractFreeWithoutKeySourceFailsClosed pins both halves of
// the hole the integration battery exposed:
//   - Required with NO key source (no contracts, no fetcher) must fail
//     closed: the identity proof can never verify, the cipher never comes
//     up, and the entry gate refuses application data — nothing is ever
//     delivered, plaintext or otherwise.
//   - Opportunistic in the same setup DELIVERS (plaintext): the silent
//     fallback that masked this hole for the battery's entire life.
func TestRequiredContractFreeWithoutKeySourceFailsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Required: fail closed
	a, b, bClientId, receivesB := contractFreeEncryptedPair(
		ctx, EncryptionModeRequired, EncryptionModeRequired, false)
	defer a.Cancel()
	defer b.Cancel()

	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "never"),
		bClientId,
		func(error) {},
		3*time.Second,
	); ok {
		t.Fatal("a Required send with no peer key source must be refused: identity can never verify, so the cipher must never come up")
	}
	select {
	case got := <-receivesB:
		t.Fatalf("fail-closed violated: %q was delivered with no verifiable peer identity", got)
	case <-time.After(2 * time.Second):
	}

	// Opportunistic: the historical silent fallback delivers plaintext
	a2, b2, b2ClientId, receivesB2 := contractFreeEncryptedPair(
		ctx, EncryptionModeOpportunistic, EncryptionModeOpportunistic, false)
	defer a2.Cancel()
	defer b2.Cancel()

	if ok := a2.SendWithTimeout(
		requiredGateFrame(t, "plaintext-fallback"),
		b2ClientId,
		func(error) {},
		30*time.Second,
	); !ok {
		t.Fatal("the opportunistic contract-free send was refused")
	}
	select {
	case got := <-receivesB2:
		AssertEqual(t, "plaintext-fallback", got)
	case <-time.After(30 * time.Second):
		t.Fatal("opportunistic contract-free delivery failed: the silent-fallback contrast no longer holds")
	}
}

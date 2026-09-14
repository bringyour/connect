package connect

// Deterministic pins for the EncryptionModeRequired fail-closed entry gate
// (DESIGNNOTES2.md §8.2 piece 1). The landed contract tests pin the two
// end-state behaviors (Required×Required establishes and delivers;
// Required×Off delivers nothing and does not wedge — §8.3); these tests pin
// the gate's TIMEOUT CONTRACT and its LIVENESS coupling, which those
// end-state tests cannot distinguish:
//
//   - timeout == 0 refuses immediately (the non-blocking contract). The
//     multi-client ping runs on a bounded timeout — an accidental wait here
//     would turn "cannot seal yet" into ping latency instead of a clean
//     refuse/rotate signal.
//   - a bounded timeout refuses AT the budget, and the refused pack is
//     UNSENT: the Off peer (who would deliver any plaintext that reached it)
//     receives nothing, ever. This is the fail-closed half: budget exhaustion
//     must never degrade to plaintext.
//   - a parked send survives its own sequence's idle timeout and delivers
//     once establishment lands (the gate "holds the idle condition open" —
//     §8.2). Idle reaping the sequence mid-park would cancel the session
//     mid-handshake and refuse sends that a moment more patience would have
//     sealed.
//
// Known deliberate gaps, documented rather than flakily tested here: the
// write-layer backstop (§8.2 piece 4's race guard) is structurally
// unreachable once the entry gate holds, and the contract-open ForceUnwrapped
// resend pin (§8.2 piece 3) has no public seam to force a resend
// deterministically — both are exercised indirectly by
// TestRequiredEncryptionEstablishesAndDelivers.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// requiredGatePair wires the standard two-client conditioned-loopback harness
// (the transfer_encrypt_contract_test idiom) with per-side encryption modes.
// When attachB is false, b's transports are NOT attached; the caller attaches
// them later with the returned attach func to control establishment timing.
func requiredGatePair(
	ctx context.Context,
	aMode EncryptionMode,
	bMode EncryptionMode,
	mutateSettings func(*ClientSettings),
	attachB bool,
) (a *Client, b *Client, aClientId Id, bClientId Id, attachBTransports func(), receivesB chan string) {
	aClientId = NewId()
	bClientId = NewId()

	aSend := make(chan []byte)
	bSend := make(chan []byte)

	_, bReceive := newConditioner(ctx, aSend)
	_, aReceive := newConditioner(ctx, bSend)

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
		s.EncryptionSettings.TlsTimeout = 30 * time.Second
		s.EncryptionSettings.EncryptionControlUseCompanion = true
		if mutateSettings != nil {
			mutateSettings(s)
		}
		return s
	}

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

	a = NewClient(ctx, aClientId, aOob, makeSettings(aMode))
	a.RouteManager().UpdateTransport(aSendTransport, []Route{aSend})
	a.RouteManager().UpdateTransport(aReceiveTransport, []Route{aReceive})
	blackholeControlId(ctx, a.RouteManager())
	a.ContractManager().SetProvideModes(provideModes)

	b = NewClient(ctx, bClientId, bOob, makeSettings(bMode))
	blackholeControlId(ctx, b.RouteManager())
	b.ContractManager().SetProvideModes(provideModes)

	attachBTransports = func() {
		b.RouteManager().UpdateTransport(bSendTransport, []Route{bSend})
		b.RouteManager().UpdateTransport(bReceiveTransport, []Route{bReceive})
	}
	if attachB {
		attachBTransports()
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

func requiredGateFrame(t *testing.T, label string) *protocol.Frame {
	m := &protocol.SimpleMessage{Content: label}
	frame, err := ToFrame(m, DefaultProtocolVersion)
	if err != nil {
		t.Fatalf("frame: %v", err)
	}
	return frame
}

// TestRequiredGateNonBlockingSendRefusesPreCipher: with the cipher not (and
// never) established, a timeout==0 send must refuse immediately — not poll,
// not park. The bound is deliberately far above scheduler noise and far below
// the gate's establishment scale (TlsTimeout 30s).
func TestRequiredGateNonBlockingSendRefusesPreCipher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	a, b, _, bClientId, _, receivesB := requiredGatePair(
		ctx, EncryptionModeRequired, EncryptionModeOff, nil, true)
	defer a.Cancel()
	defer b.Cancel()

	// initialize the session: a parked send (infinite timeout) starts the
	// held handshake against the Off peer, exactly the FailsClosed state
	parked := make(chan bool, 1)
	go func() {
		parked <- a.SendWithTimeout(
			requiredGateFrame(t, "parked"),
			bClientId,
			func(error) {},
			-1,
		)
	}()
	select {
	case ok := <-parked:
		t.Fatalf("send against an Off peer must park at the gate, returned %t", ok)
	case <-time.After(1 * time.Second):
	}

	startTime := time.Now()
	ok := a.SendWithTimeout(
		requiredGateFrame(t, "nonblocking"),
		bClientId,
		func(error) {},
		0,
	)
	elapsed := time.Since(startTime)
	if ok {
		t.Fatal("a timeout==0 send with no cipher must refuse")
	}
	if 2*time.Second <= elapsed {
		t.Fatalf("the non-blocking refuse took %s: the gate waited instead of refusing immediately", elapsed)
	}

	select {
	case got := <-receivesB:
		t.Fatalf("fail-closed violated: the Off peer received %q", got)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestRequiredGateBoundedBudgetRefusesUnsent: a bounded-timeout send against
// a never-establishing peer must refuse at (not far past) its budget, and the
// refused pack must be UNSENT — the Off peer, which would deliver any
// plaintext that reached it, receives nothing. Budget exhaustion degrading to
// plaintext is the exact fail-open this gate exists to close.
func TestRequiredGateBoundedBudgetRefusesUnsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	a, b, _, bClientId, _, receivesB := requiredGatePair(
		ctx, EncryptionModeRequired, EncryptionModeOff, nil, true)
	defer a.Cancel()
	defer b.Cancel()

	budget := 400 * time.Millisecond
	startTime := time.Now()
	ok := a.SendWithTimeout(
		requiredGateFrame(t, "budgeted"),
		bClientId,
		func(error) {},
		budget,
	)
	elapsed := time.Since(startTime)
	if ok {
		t.Fatal("a bounded send with no cipher must refuse at its budget")
	}
	if elapsed < budget-50*time.Millisecond {
		t.Fatalf("refused after %s, before the %s budget: the caller paid for a wait the gate did not perform", elapsed, budget)
	}
	if 8*time.Second <= elapsed {
		t.Fatalf("refused after %s: the gate overran a %s budget", elapsed, budget)
	}

	// the refusal must mean unsent, not sent-in-plaintext
	select {
	case got := <-receivesB:
		t.Fatalf("fail-closed violated: the Off peer received %q after the budget refuse", got)
	case <-time.After(1 * time.Second):
	}
}

// TestRequiredGateParkedSendSurvivesIdleAndDelivers: the gate wait must hold
// the sequence's idle condition open. The send parks far longer than the
// sequence's own IdleTimeout while the peer's transports are deliberately
// attached late; if parking did not hold the idle condition, the sequence
// (and the session riding it) would be reaped mid-handshake and the send
// refused. Once the peer attaches, establishment completes and the parked
// send delivers — sealed, end to end.
func TestRequiredGateParkedSendSurvivesIdleAndDelivers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	attachDelay := 1500 * time.Millisecond
	a, b, _, bClientId, attachB, receivesB := requiredGatePair(
		ctx, EncryptionModeRequired, EncryptionModeRequired,
		func(s *ClientSettings) {
			// idle far shorter than the park: the wait must keep it alive
			s.SendBufferSettings.IdleTimeout = 300 * time.Millisecond
		},
		false,
	)
	defer a.Cancel()
	defer b.Cancel()

	sent := make(chan bool, 1)
	startTime := time.Now()
	go func() {
		sent <- a.SendWithTimeout(
			requiredGateFrame(t, "late-establishment"),
			bClientId,
			func(error) {},
			30*time.Second,
		)
	}()

	select {
	case ok := <-sent:
		t.Fatalf("send returned %t after %s, before the peer was even attached", ok, time.Since(startTime))
	case <-time.After(attachDelay):
	}
	attachB()

	select {
	case ok := <-sent:
		if !ok {
			t.Fatalf(
				"the parked send was refused after %s (idle timeout 300ms, peer attached at %s): the gate wait did not keep the sequence alive through establishment",
				time.Since(startTime), attachDelay,
			)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the parked send neither delivered nor refused after the peer attached")
	}

	select {
	case got := <-receivesB:
		AssertEqual(t, "late-establishment", got)
	case <-time.After(15 * time.Second):
		t.Fatal("the peer never received the parked send after establishment")
	}
	_ = fmt.Sprintf
}

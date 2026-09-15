package connect

// Tests for receiver-visible sequence discriminators (Pack fields 10/11/12,
// 2026-08). The sender stamps its local force_stream, companion_contract, and
// bounded logical-lane identity on every Pack so distinct sequences coexist
// instead of superseding each other.
//
// Edge cases covered here:
//   - older clients: a Pack without discriminator fields decodes to
//     false/false/logical-zero, byte-identical to the legacy behavior
//   - the historical force-stream/companion sequences remain isolated when
//     interleaved; logical-lane loss isolation is covered separately
//   - sender and receiver each hold one live sequence per discriminator
// Same-lane supersession (the legacy reset semantic) is covered by
// TestSendReceiveSenderReset, which is unchanged by lanes.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestPackLaneCodecLegacyAbsent verifies both receive decoders map a Pack
// with no lane fields (what a pre-lane peer emits) to the false/false lane,
// and round-trip Packs with lanes set.
func TestPackLaneCodecLegacyAbsent(t *testing.T) {
	legacy := &protocol.Pack{
		MessageId:      NewId().Bytes(),
		SequenceId:     NewId().Bytes(),
		SequenceNumber: 7,
	}
	legacyBytes, err := proto.Marshal(legacy)
	AssertEqual(t, err, nil)

	pack, ok := decodePack(legacyBytes)
	AssertEqual(t, ok, true)
	AssertEqual(t, pack.ForceStream, false)
	AssertEqual(t, pack.CompanionContract, false)
	AssertEqual(t, pack.LogicalLane, uint32(0))
	returnDecodedPackMessageBytes(pack)

	owner, ok := decodePackOwned(legacyBytes)
	AssertEqual(t, ok, true)
	AssertEqual(t, owner.pack.ForceStream, false)
	AssertEqual(t, owner.pack.CompanionContract, false)
	AssertEqual(t, owner.pack.LogicalLane, uint32(0))
	owner.release()

	for _, lane := range []struct {
		forceStream       bool
		companionContract bool
		logicalLane       uint32
	}{
		{forceStream: true, logicalLane: 1},
		{companionContract: true, logicalLane: 8},
		{forceStream: true, companionContract: true, logicalLane: 4},
	} {
		laned := &protocol.Pack{
			MessageId:         NewId().Bytes(),
			SequenceId:        NewId().Bytes(),
			SequenceNumber:    9,
			ForceStream:       lane.forceStream,
			CompanionContract: lane.companionContract,
			LogicalLane:       lane.logicalLane,
		}
		lanedBytes, err := proto.Marshal(laned)
		AssertEqual(t, err, nil)

		pack, ok := decodePack(lanedBytes)
		AssertEqual(t, ok, true)
		AssertEqual(t, pack.ForceStream, lane.forceStream)
		AssertEqual(t, pack.CompanionContract, lane.companionContract)
		AssertEqual(t, pack.LogicalLane, lane.logicalLane)
		returnDecodedPackMessageBytes(pack)

		owner, ok := decodePackOwned(lanedBytes)
		AssertEqual(t, ok, true)
		AssertEqual(t, owner.pack.ForceStream, lane.forceStream)
		AssertEqual(t, owner.pack.CompanionContract, lane.companionContract)
		AssertEqual(t, owner.pack.LogicalLane, lane.logicalLane)
		owner.release()
	}
}

// TestSendReceiveParallelLanes drives every legal lane concurrently between
// one client pair, interleaving lanes per message — the exact alternation
// that used to flap the wire-indistinguishable retire and starve delivery.
// (companion+force-stream combined is platform-rejected, so the legal lanes
// are default, force-stream, and companion.)
func TestSendReceiveParallelLanes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	type lane struct {
		name string
		fs   bool
		cc   bool
		opts []any
	}
	lanes := []*lane{
		{name: "default", fs: false, cc: false, opts: nil},
		{name: "stream", fs: true, cc: false, opts: []any{ForceStream()}},
		{name: "companion", fs: false, cc: true, opts: []any{CompanionContract()}},
	}
	n := 48
	timeout := 5 * time.Minute

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aClientId := NewId()
	bClientId := NewId()

	aSend := make(chan []byte)
	bSend := make(chan []byte)

	// no conditioning: lane isolation is structural, not loss-dependent
	_, bReceive := newConditioner(ctx, aSend)
	_, aReceive := newConditioner(ctx, bSend)

	aSendTransport := NewSendGatewayTransport()
	aReceiveTransport := NewReceiveGatewayTransport()
	bSendTransport := NewSendGatewayTransport()
	bReceiveTransport := NewReceiveGatewayTransport()

	provideModes := map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
	}

	newSettings := func() *ClientSettings {
		clientSettings := DefaultClientSettings()
		clientSettings.SendBufferSettings.AckTimeout = 300 * time.Second
		clientSettings.SendBufferSettings.IdleTimeout = 300 * time.Second
		clientSettings.ReceiveBufferSettings.GapTimeout = 300 * time.Second
		clientSettings.ReceiveBufferSettings.IdleTimeout = 300 * time.Second
		clientSettings.ContractManagerSettings.LegacyCreateContract = true
		applyTestEncryptionSettings(clientSettings, encryptionModeOff)
		return clientSettings
	}

	a := NewClient(ctx, aClientId, NewNoContractClientOob(), newSettings())
	defer a.Cancel()
	a.RouteManager().UpdateTransport(aSendTransport, []Route{aSend})
	a.RouteManager().UpdateTransport(aReceiveTransport, []Route{aReceive})
	a.ContractManager().SetProvideModes(provideModes)

	b := NewClient(ctx, bClientId, NewNoContractClientOob(), newSettings())
	defer b.Cancel()
	b.RouteManager().UpdateTransport(bSendTransport, []Route{bSend})
	b.RouteManager().UpdateTransport(bReceiveTransport, []Route{bReceive})
	b.ContractManager().SetProvideModes(provideModes)

	type laneReceive struct {
		lane        string
		index       int
		source      TransferPath
		transferKey TransferKey
	}
	receives := make(chan laneReceive, 3*n)
	asyncErrors := make(chan error, 1)
	recordAsyncError := func(err error) {
		select {
		case asyncErrors <- err:
		default:
		}
	}
	b.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			m, err := FromFrame(frame)
			if err != nil {
				recordAsyncError(fmt.Errorf("decode lane receive: %w", err))
				return
			}
			switch v := m.(type) {
			case *protocol.SimpleMessage:
				parts := strings.SplitN(v.Content, " ", 2)
				index, err := strconv.Atoi(parts[1])
				if err != nil {
					recordAsyncError(fmt.Errorf("decode lane index: %w", err))
					return
				}
				receive := laneReceive{
					lane:        parts[0],
					index:       index,
					source:      source,
					transferKey: peer.TransferKey,
				}
				select {
				case receives <- receive:
				default:
					recordAsyncError(fmt.Errorf("lane receive collector overflow"))
				}
			}
		}
	})

	// the platform serves CreateContract for any contract key: feed every
	// lane's key
	for _, l := range lanes {
		for range 4 {
			err := a.ContractManager().HandleControlFrame(
				ContractKey{
					Destination:       DestinationId(bClientId),
					ForceStream:       l.fs,
					CompanionContract: l.cc,
				},
				requireContractResult(
					protocol.ProvideMode_Network,
					b.ContractManager().RequireProvideSecretKey(protocol.ProvideMode_Network),
					aClientId,
					bClientId,
				),
			)
			AssertEqual(t, err, nil)
		}
	}

	acks := make(chan error, 3*n)
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		// interleave lanes per message: the historical flap trigger
		for i := 0; i < n; i += 1 {
			for _, l := range lanes {
				message := &protocol.SimpleMessage{
					Content: fmt.Sprintf("%s %d", l.name, i),
				}
				frame, err := ToFrame(message, DefaultProtocolVersion)
				if err != nil {
					recordAsyncError(fmt.Errorf("encode lane %s message %d: %w", l.name, i, err))
					return
				}
				success := a.SendWithTimeout(frame, bClientId, func(err error) {
					select {
					case acks <- err:
					default:
						recordAsyncError(fmt.Errorf("lane ack collector overflow: %v", err))
					}
				}, -1, l.opts...)
				if !success {
					recordAsyncError(fmt.Errorf("send lane %s message %d", l.name, i))
					return
				}
			}
		}
	}()

	// exact-once, in-order delivery per lane
	nextIndex := map[string]int{}
	ackCount := 0
	receiveCount := 0
	deadline := time.Now().Add(timeout)
	for receiveCount < 3*n || ackCount < 3*n {
		progressTimeout := time.Minute
		if 0 < receiveCount {
			progressTimeout = time.Until(deadline)
		}
		select {
		case <-ctx.Done():
			return
		case r := <-receives:
			AssertEqual(t, r.index, nextIndex[r.lane])
			var expectedLane *lane
			for _, candidate := range lanes {
				if candidate.name == r.lane {
					expectedLane = candidate
					break
				}
			}
			if expectedLane == nil {
				t.Fatalf("unknown receive lane %q", r.lane)
			}
			AssertEqual(t, SourceId(aClientId), r.source)
			AssertEqual(t, expectedLane.fs, r.transferKey.ForceStream)
			AssertEqual(t, expectedLane.cc, r.transferKey.CompanionContract)
			AssertEqual(t, protocol.SequenceRole_SequenceRoleServer, r.transferKey.EncryptionRole)
			AssertEqual(t, expectedLane.cc, r.transferKey.EncryptionCompanion)
			nextIndex[r.lane] = r.index + 1
			receiveCount += 1
		case err := <-acks:
			AssertEqual(t, err, nil)
			ackCount += 1
		case asyncErr := <-asyncErrors:
			t.Fatalf("asynchronous lane worker: %v", asyncErr)
		case <-time.After(progressTimeout):
			t.Fatalf(
				"lane starvation: receives=%d/%d acks=%d/%d next=%v",
				receiveCount, 3*n, ackCount, 3*n, nextIndex,
			)
		}
	}
	select {
	case <-sendDone:
	case asyncErr := <-asyncErrors:
		t.Fatalf("asynchronous lane sender: %v", asyncErr)
	case <-time.After(timeout):
		t.Fatal("lane sender did not finish")
	}
	for _, l := range lanes {
		AssertEqual(t, nextIndex[l.name], n)
	}

	// sender holds one live sequence per lane, with distinct wire identities
	func() {
		a.sendBuffer.mutex.Lock()
		defer a.sendBuffer.mutex.Unlock()
		laneCount := map[[2]bool]int{}
		for id := range a.sendBuffer.sendSequences {
			if id.Destination == bClientId && id.EncryptionRole == sequenceTlsRoleClient {
				laneCount[[2]bool{id.ForceStream, id.CompanionContract}] += 1
			}
		}
		for _, l := range lanes {
			AssertEqual(t, laneCount[[2]bool{l.fs, l.cc}], 1)
		}
		wireCount := 0
		for wireId := range a.sendBuffer.wireSendSequences {
			if wireId.Destination == bClientId && wireId.EncryptionRole == sequenceTlsRoleClient {
				wireCount += 1
			}
		}
		AssertEqual(t, wireCount, len(lanes))
	}()

	// receiver holds one head slot per lane for the source
	func() {
		b.receiveBuffer.mutex.Lock()
		defer b.receiveBuffer.mutex.Unlock()
		laneCount := map[[2]bool]int{}
		for headKey := range b.receiveBuffer.headReceiveSequenceIds {
			if headKey.Source == SourceId(aClientId) {
				laneCount[[2]bool{headKey.ForceStream, headKey.CompanionContract}] += 1
			}
		}
		for _, l := range lanes {
			AssertEqual(t, laneCount[[2]bool{l.fs, l.cc}], 1)
		}
	}()
}

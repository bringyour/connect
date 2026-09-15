package connect

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestStreamManagerStreamLifecycle(t *testing.T) {
	// stream control frames from the platform drive the stream buffer:
	// open with destination only (this client is the stream source),
	// open with source only (this client is the stream destination),
	// open with both (this client is an intermediary hop),
	// duplicate opens are idempotent,
	// reopening a stream id with different endpoints evicts the old sequence,
	// close cancels the stream,
	// and reset closes all open streams and opens the listed set

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Close()
	// The lifecycle test exercises an inbound endpoint stream. Register Public
	// provide explicitly; clients with no provider mode now reject such stale
	// StreamOpen state before allocating a P2P transport.
	client.ContractManager().SetProvideModes(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Public: true,
	})

	streamManager := client.streamManager
	openTracker := newStreamOpenTestTracker(streamManager.streamBuffer)

	mustFrame := func(message proto.Message) *protocol.Frame {
		frame, err := ToFrame(message, DefaultProtocolVersion)
		AssertEqual(t, err, nil)
		return frame
	}

	receiveControl := func(message proto.Message) {
		streamManager.Receive(SourceId(ControlId), []*protocol.Frame{mustFrame(message)}, Peer{})
	}

	streamOpen := func(sourceId *Id, destinationId *Id, streamId Id) *protocol.StreamOpen {
		streamOpen := &protocol.StreamOpen{
			StreamId: streamId.Bytes(),
		}
		if sourceId != nil {
			streamOpen.SourceId = sourceId.Bytes()
		}
		if destinationId != nil {
			streamOpen.DestinationId = destinationId.Bytes()
		}
		return streamOpen
	}

	getSequence := func(streamId Id) *StreamSequence {
		streamBuffer := streamManager.streamBuffer
		streamBuffer.mutex.Lock()
		defer streamBuffer.mutex.Unlock()
		return streamBuffer.streamSequencesByStreamId[streamId]
	}

	hasStreamSequenceId := func(sourceId *Id, destinationId *Id, streamId Id) bool {
		streamBuffer := streamManager.streamBuffer
		streamBuffer.mutex.Lock()
		defer streamBuffer.mutex.Unlock()
		_, ok := streamBuffer.streamSequences[newStreamSequenceId(sourceId, destinationId, streamId)]
		return ok
	}

	// open with destination only: this client is the stream source
	destinationId := NewId()
	endpointStreamId := NewId()
	endpointPublication := openTracker.expectPublication(endpointStreamId)
	receiveControl(streamOpen(nil, &destinationId, endpointStreamId))
	endpointPublication.wait(t)
	AssertEqual(t, true, streamManager.IsStreamOpen(endpointStreamId))
	endpointPublication.resume()

	// open with source only: this client is the stream destination
	sourceId := NewId()
	sourceStreamId := NewId()
	sourcePublication := openTracker.expectPublication(sourceStreamId)
	receiveControl(streamOpen(&sourceId, nil, sourceStreamId))
	sourcePublication.wait(t)
	AssertEqual(t, true, streamManager.IsStreamOpen(sourceStreamId))

	// open with both: this client is an intermediary hop
	intermediaryStreamId := NewId()
	intermediaryPublication := openTracker.expectPublication(intermediaryStreamId)
	receiveControl(streamOpen(&sourceId, &destinationId, intermediaryStreamId))
	intermediaryPublication.wait(t)
	AssertEqual(t, true, streamManager.IsStreamOpen(intermediaryStreamId))

	// a duplicate open leaves the existing sequence in place
	sequence := getSequence(endpointStreamId)
	AssertEqual(t, true, sequence != nil)
	duplicateCompletion := openTracker.expectCompletion(endpointStreamId)
	receiveControl(streamOpen(nil, &destinationId, endpointStreamId))
	waitForStreamOpenTestCompletion(t, duplicateCompletion)
	AssertEqual(t, true, sequence == getSequence(endpointStreamId))

	// reopening the stream id with different endpoints cancels the old sequence
	otherDestinationId := NewId()
	replacementPublication := openTracker.expectPublication(endpointStreamId)
	evictedRemoval := openTracker.expectRemoval(endpointStreamId)
	receiveControl(streamOpen(nil, &otherDestinationId, endpointStreamId))
	replacementPublication.wait(t)
	AssertEqual(t, true, streamManager.IsStreamOpen(endpointStreamId))
	evictedSequence := sequence
	sequence = getSequence(endpointStreamId)
	AssertEqual(t, true, sequence != nil)
	AssertEqual(t, true, evictedSequence != sequence)
	AssertEqual(t, true, evictedSequence.ctx.Err() != nil)
	// the evicted sequence is asynchronously cleaned up
	waitForStreamLifecycleSignal(t, evictedRemoval, "evicted stream did not leave its indexes")
	AssertEqual(t, false, hasStreamSequenceId(nil, &destinationId, endpointStreamId))

	// close cancels the stream
	intermediaryRemoval := openTracker.expectRemoval(intermediaryStreamId)
	receiveControl(&protocol.StreamClose{
		StreamId: intermediaryStreamId.Bytes(),
	})
	intermediaryPublication.resume()
	waitForStreamLifecycleSignal(t, intermediaryRemoval, "closed intermediary stream did not leave its indexes")
	AssertEqual(t, false, streamManager.IsStreamOpen(intermediaryStreamId))

	// reset reconciles: relisted streams keep their sequences,
	// unlisted streams close, and newly listed streams open
	resetStreamId := NewId()
	sourceRemoval := openTracker.expectRemoval(sourceStreamId)
	resetPublication := openTracker.expectPublication(resetStreamId)
	receiveControl(&protocol.StreamReset{
		Streams: []*protocol.StreamOpen{
			streamOpen(nil, &otherDestinationId, endpointStreamId),
			streamOpen(nil, &destinationId, resetStreamId),
		},
	})
	resetPublication.wait(t)
	AssertEqual(t, true, streamManager.IsStreamOpen(endpointStreamId))
	AssertEqual(t, true, streamManager.IsStreamOpen(resetStreamId))
	sourcePublication.resume()
	replacementPublication.resume()
	resetPublication.resume()
	waitForStreamLifecycleSignal(t, sourceRemoval, "unlisted reset stream did not leave its indexes")
	AssertEqual(t, false, streamManager.IsStreamOpen(sourceStreamId))
	// the relisted stream keeps its live sequence across the reset,
	// so its p2p transports survive a resident migration
	AssertEqual(t, true, sequence == getSequence(endpointStreamId))
	AssertEqual(t, true, sequence.ctx.Err() == nil)

	// an empty reset cancels everything (the legacy reset behavior)
	endpointRemoval := openTracker.expectRemoval(endpointStreamId)
	resetRemoval := openTracker.expectRemoval(resetStreamId)
	receiveControl(&protocol.StreamReset{})
	waitForStreamLifecycleSignal(t, endpointRemoval, "empty reset endpoint stream did not leave its indexes")
	waitForStreamLifecycleSignal(t, resetRemoval, "empty reset listed stream did not leave its indexes")
	AssertEqual(t, false, streamManager.IsStreamOpen(endpointStreamId))
	AssertEqual(t, false, streamManager.IsStreamOpen(resetStreamId))
	AssertEqual(t, true, sequence.ctx.Err() != nil)
}

// TestStreamManagerProvidePolicyRetiresStaleInboundStreams verifies that
// reducing/pausing provider modes tears down existing public endpoint streams
// and rejects stale relists, without disrupting outbound client streams or
// allowed same-network peers. Intermediary streams are public relay work and
// are retired with public providing.
func TestStreamManagerProvidePolicyRetiresStaleInboundStreams(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.ProviderStreamPolicy = true
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Close()

	streamManager := client.streamManager
	openTracker := newStreamOpenTestTracker(streamManager.streamBuffer)
	mustFrame := func(message proto.Message) *protocol.Frame {
		frame, err := ToFrame(message, DefaultProtocolVersion)
		AssertEqual(t, err, nil)
		return frame
	}
	receiveOpen := func(sourceId *Id, destinationId *Id, streamId Id) {
		message := &protocol.StreamOpen{StreamId: streamId.Bytes()}
		if sourceId != nil {
			message.SourceId = sourceId.Bytes()
		}
		if destinationId != nil {
			message.DestinationId = destinationId.Bytes()
		}
		streamManager.Receive(SourceId(ControlId), []*protocol.Frame{mustFrame(message)}, Peer{})
	}
	openAndHold := func(sourceId *Id, destinationId *Id, streamId Id) *streamOpenTestPublication {
		publication := openTracker.expectPublication(streamId)
		receiveOpen(sourceId, destinationId, streamId)
		publication.wait(t)
		return publication
	}
	publicSourceId := NewId()
	networkSourceId := NewId()
	destinationId := NewId()
	publicStreamId := NewId()
	networkStreamId := NewId()
	outboundStreamId := NewId()
	intermediaryStreamId := NewId()

	_, err := client.peerManager.updatePeers(&protocol.NetworkPeersUpdate{
		Peers: []*protocol.NetworkPeer{
			{
				ClientId:     networkSourceId.Bytes(),
				ProvideModes: []protocol.ProvideMode{protocol.ProvideMode_Network},
			},
			{
				ClientId:     destinationId.Bytes(),
				ProvideModes: []protocol.ProvideMode{protocol.ProvideMode_Network},
			},
		},
	})
	AssertEqual(t, err, nil)

	client.ContractManager().SetProvideModes(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Public:  true,
		protocol.ProvideMode_Network: true,
	})
	publicPublication := openAndHold(&publicSourceId, nil, publicStreamId)
	networkPublication := openAndHold(&networkSourceId, nil, networkStreamId)
	outboundPublication := openAndHold(nil, &destinationId, outboundStreamId)
	intermediaryPublication := openAndHold(&publicSourceId, &destinationId, intermediaryStreamId)
	AssertEqual(t, true, streamManager.IsStreamOpen(publicStreamId))
	AssertEqual(t, true, streamManager.IsStreamOpen(networkStreamId))
	AssertEqual(t, true, streamManager.IsStreamOpen(outboundStreamId))
	AssertEqual(t, true, streamManager.IsStreamOpen(intermediaryStreamId))
	networkSequence := streamLifecycleSequence(client, networkStreamId)
	outboundSequence := streamLifecycleSequence(client, outboundStreamId)
	publicPublication.resume()
	intermediaryPublication.resume()

	// Network-only retires a public endpoint and public intermediary, but not a
	// known network peer or an outbound stream on which this client is source.
	publicRemoval := openTracker.expectRemoval(publicStreamId)
	intermediaryRemoval := openTracker.expectRemoval(intermediaryStreamId)
	client.ContractManager().SetProvideModes(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
	})
	waitForStreamLifecycleSignal(t, publicRemoval, "public stream did not leave its indexes after Network-only policy")
	waitForStreamLifecycleSignal(t, intermediaryRemoval, "relay stream did not leave its indexes after Network-only policy")
	AssertEqual(t, false, streamManager.IsStreamOpen(publicStreamId))
	AssertEqual(t, false, streamManager.IsStreamOpen(intermediaryStreamId))
	AssertEqual(t, true, streamManager.IsStreamOpen(networkStreamId))
	AssertEqual(t, true, streamManager.IsStreamOpen(outboundStreamId))
	AssertEqual(t, true, networkSequence.ctx.Err() == nil)
	AssertEqual(t, true, outboundSequence.ctx.Err() == nil)

	// A delayed StreamOpen or StreamReset from the old public registration
	// must not resurrect its P2P admission loop.
	relistedPublicStreamId := NewId()
	relistedRelayStreamId := NewId()
	receiveOpen(&publicSourceId, nil, relistedPublicStreamId)
	receiveOpen(&publicSourceId, &destinationId, relistedRelayStreamId)
	AssertEqual(t, false, streamManager.IsStreamOpen(relistedPublicStreamId))
	AssertEqual(t, false, streamManager.IsStreamOpen(relistedRelayStreamId))
	streamManager.Receive(
		SourceId(ControlId),
		[]*protocol.Frame{mustFrame(&protocol.StreamReset{
			Streams: []*protocol.StreamOpen{
				{SourceId: publicSourceId.Bytes(), StreamId: relistedPublicStreamId.Bytes()},
				{SourceId: publicSourceId.Bytes(), DestinationId: destinationId.Bytes(), StreamId: relistedRelayStreamId.Bytes()},
				{SourceId: networkSourceId.Bytes(), StreamId: networkStreamId.Bytes()},
				{DestinationId: destinationId.Bytes(), StreamId: outboundStreamId.Bytes()},
			},
		})},
		Peer{},
	)
	AssertEqual(t, false, streamManager.IsStreamOpen(relistedPublicStreamId))
	AssertEqual(t, false, streamManager.IsStreamOpen(relistedRelayStreamId))
	AssertEqual(t, true, streamManager.IsStreamOpen(networkStreamId))
	AssertEqual(t, true, streamManager.IsStreamOpen(outboundStreamId))
	AssertEqual(t, true, networkSequence.ctx.Err() == nil)
	AssertEqual(t, true, outboundSequence.ctx.Err() == nil)

	// Pause has the same public/FF suppression while retaining Network.
	client.ContractManager().SetProvideModes(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Public:  true,
		protocol.ProvideMode_Network: true,
	})
	publicPublication = openAndHold(&publicSourceId, nil, publicStreamId)
	AssertEqual(t, true, streamManager.IsStreamOpen(publicStreamId))
	pausedPublicSequence := streamLifecycleSequence(client, publicStreamId)
	pausedPublicRemoval := openTracker.expectRemoval(publicStreamId)
	client.ContractManager().SetProvidePaused(true)
	AssertEqual(t, true, pausedPublicSequence.ctx.Err() != nil)
	publicPublication.resume()
	waitForStreamLifecycleSignal(t, pausedPublicRemoval, "paused public stream did not leave its indexes")
	AssertEqual(t, false, streamManager.IsStreamOpen(publicStreamId))
	AssertEqual(t, true, streamManager.IsStreamOpen(networkStreamId))

	// No provider modes ("never") retires every provider-owned direction,
	// including a companion/return stream on which this client is source.
	client.ContractManager().SetProvidePaused(false)
	networkRemoval := openTracker.expectRemoval(networkStreamId)
	outboundRemoval := openTracker.expectRemoval(outboundStreamId)
	client.ContractManager().SetProvideModes(map[protocol.ProvideMode]bool{})
	AssertEqual(t, true, networkSequence.ctx.Err() != nil)
	AssertEqual(t, true, outboundSequence.ctx.Err() != nil)
	networkPublication.resume()
	outboundPublication.resume()
	waitForStreamLifecycleSignal(t, networkRemoval, "disabled network stream did not leave its indexes")
	waitForStreamLifecycleSignal(t, outboundRemoval, "disabled outbound stream did not leave its indexes")
	AssertEqual(t, false, streamManager.IsStreamOpen(networkStreamId))
	AssertEqual(t, false, streamManager.IsStreamOpen(outboundStreamId))
}

// TestStreamManagerNetworkPeerBatchOrderingAndDisconnectRetirement covers the
// platform's real control ordering. Peer Reset/Update and StreamReset may
// share one received batch; peer membership must be visible before strict
// Network stream authorization. A later disconnect must retire that stream
// without waiting for a provide-mode change or another stream relist.
func TestStreamManagerNetworkPeerBatchOrderingAndDisconnectRetirement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.ProviderStreamPolicy = true
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Close()
	openTracker := newStreamOpenTestTracker(client.streamManager.streamBuffer)
	client.ContractManager().SetProvideModes(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
	})

	mustFrame := func(message proto.Message) *protocol.Frame {
		frame, err := ToFrame(message, DefaultProtocolVersion)
		AssertEqual(t, err, nil)
		return frame
	}

	peerId := NewId()
	streamId := NewId()
	publication := openTracker.expectPublication(streamId)
	client.receive(
		SourceId(ControlId),
		[]*protocol.Frame{
			mustFrame(&protocol.NetworkPeersReset{}),
			mustFrame(&protocol.NetworkPeersUpdate{
				Peers: []*protocol.NetworkPeer{{
					ClientId:     peerId.Bytes(),
					ProvideModes: []protocol.ProvideMode{protocol.ProvideMode_Network},
				}},
			}),
			mustFrame(&protocol.StreamReset{
				Streams: []*protocol.StreamOpen{{
					SourceId: peerId.Bytes(),
					StreamId: streamId.Bytes(),
				}},
			}),
		},
		Peer{},
	)
	publication.wait(t)
	if !client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("same-batch Network stream was rejected before peer state became visible")
	}
	sequence := streamLifecycleSequence(client, streamId)

	disconnectTime := uint64(time.Now().UnixMilli())
	removal := openTracker.expectRemoval(streamId)
	client.receive(
		SourceId(ControlId),
		[]*protocol.Frame{mustFrame(&protocol.NetworkPeersUpdate{
			Peers: []*protocol.NetworkPeer{{
				ClientId:       peerId.Bytes(),
				DisconnectTime: &disconnectTime,
			}},
		})},
		Peer{},
	)
	if sequence == nil || sequence.ctx.Err() == nil {
		t.Fatal("disconnected Network peer did not cancel its provider stream")
	}
	publication.resume()
	waitForStreamLifecycleSignal(t, removal, "disconnected Network stream did not leave its indexes")
	if client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("disconnected Network peer retained stale provider stream")
	}
}

func TestStreamManagerPublicProviderRetiresKnownDisconnectedPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.ProviderStreamPolicy = true
	settings.Log = NewNoopLogger()
	// Keep the test at the stream-lifecycle boundary: setup is intentionally
	// refused by a zero-byte budget rather than constructing real Pion state.
	settings.WebRtcSettings.MemoryBudget = NewTransferMemoryBudget(0)
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Close()
	openTracker := newStreamOpenTestTracker(client.streamManager.streamBuffer)
	client.ContractManager().SetProvideModes(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
		protocol.ProvideMode_Public:  true,
	})

	mustFrame := func(message proto.Message) *protocol.Frame {
		frame, err := ToFrame(message, DefaultProtocolVersion)
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
	peerId := NewId()
	streamId := NewId()
	publication := openTracker.expectPublication(streamId)
	client.receive(
		SourceId(ControlId),
		[]*protocol.Frame{
			mustFrame(&protocol.NetworkPeersUpdate{
				Peers: []*protocol.NetworkPeer{{
					ClientId:     peerId.Bytes(),
					ProvideModes: []protocol.ProvideMode{protocol.ProvideMode_Network},
				}},
			}),
			mustFrame(&protocol.StreamOpen{
				SourceId: peerId.Bytes(),
				StreamId: streamId.Bytes(),
			}),
		},
		Peer{},
	)
	publication.wait(t)
	if !client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("connected peer stream did not open on public-capable provider")
	}
	sequence := streamLifecycleSequence(client, streamId)

	disconnectTime := uint64(time.Now().UnixMilli())
	removal := openTracker.expectRemoval(streamId)
	client.receive(
		SourceId(ControlId),
		[]*protocol.Frame{mustFrame(&protocol.NetworkPeersUpdate{
			Peers: []*protocol.NetworkPeer{{
				ClientId:       peerId.Bytes(),
				DisconnectTime: &disconnectTime,
			}},
		})},
		Peer{},
	)
	if sequence == nil || sequence.ctx.Err() == nil {
		t.Fatal("public provide policy did not cancel a known disconnected peer stream")
	}
	publication.resume()
	waitForStreamLifecycleSignal(t, removal, "disconnected public-provider stream did not leave its indexes")
	if client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("public provide policy retained a known disconnected peer stream")
	}

	// A delayed control replay must not resurrect the retired generation.
	client.receive(
		SourceId(ControlId),
		[]*protocol.Frame{mustFrame(&protocol.StreamOpen{
			SourceId: peerId.Bytes(),
			StreamId: streamId.Bytes(),
		})},
		Peer{},
	)
	if client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("delayed stream open resurrected a disconnected peer")
	}

	// Unknown public identities remain valid; a disconnect tombstone is not a
	// blanket requirement that every public client appear in NetworkPeers.
	publicPeerId := NewId()
	publicStreamId := NewId()
	publicPublication := openTracker.expectPublication(publicStreamId)
	client.receive(
		SourceId(ControlId),
		[]*protocol.Frame{mustFrame(&protocol.StreamOpen{
			SourceId: publicPeerId.Bytes(),
			StreamId: publicStreamId.Bytes(),
		})},
		Peer{},
	)
	publicPublication.wait(t)
	if !client.streamManager.IsStreamOpen(publicStreamId) {
		t.Fatal("public provider rejected an identity absent from NetworkPeers")
	}
	publicPublication.resume()

	// A real reconnect clears the marker before a same-batch stream relist.
	reconnectPublication := openTracker.expectPublication(streamId)
	client.receive(
		SourceId(ControlId),
		[]*protocol.Frame{
			mustFrame(&protocol.NetworkPeersUpdate{
				Peers: []*protocol.NetworkPeer{{
					ClientId:     peerId.Bytes(),
					ProvideModes: []protocol.ProvideMode{protocol.ProvideMode_Network},
				}},
			}),
			mustFrame(&protocol.StreamOpen{
				SourceId: peerId.Bytes(),
				StreamId: streamId.Bytes(),
			}),
		},
		Peer{},
	)
	reconnectPublication.wait(t)
	if !client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("peer reconnect did not permit a fresh stream generation")
	}
	reconnectPublication.resume()
}

// TestStreamManagerOrdinaryClientKeepsTransportDirectionsWithoutProviderMode
// guards selected-destination/window clients. Source-only, destination-only,
// and paired StreamOpen entries may all represent that client's own transport
// and return/companion state; none is provider authorization. Reducing provider
// modes must therefore leave all of them intact.
func TestStreamManagerOrdinaryClientKeepsTransportDirectionsWithoutProviderMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Close()
	openTracker := newStreamOpenTestTracker(client.streamManager.streamBuffer)

	sourceId := NewId()
	destinationId := NewId()
	streamIds := []Id{NewId(), NewId(), NewId()}
	opens := []*protocol.StreamOpen{
		{SourceId: sourceId.Bytes(), StreamId: streamIds[0].Bytes()},
		{DestinationId: destinationId.Bytes(), StreamId: streamIds[1].Bytes()},
		{SourceId: sourceId.Bytes(), DestinationId: destinationId.Bytes(), StreamId: streamIds[2].Bytes()},
	}
	publications := make([]*streamOpenTestPublication, 0, len(opens))
	for index, message := range opens {
		publication := openTracker.expectPublication(streamIds[index])
		frame, err := ToFrame(message, DefaultProtocolVersion)
		AssertEqual(t, err, nil)
		client.streamManager.Receive(SourceId(ControlId), []*protocol.Frame{frame}, Peer{})
		publication.wait(t)
		publications = append(publications, publication)
	}
	for _, streamId := range streamIds {
		AssertEqual(t, true, client.streamManager.IsStreamOpen(streamId))
	}

	client.ContractManager().SetProvideModes(map[protocol.ProvideMode]bool{})
	for _, streamId := range streamIds {
		AssertEqual(t, true, client.streamManager.IsStreamOpen(streamId))
		sequence := streamLifecycleSequence(client, streamId)
		AssertEqual(t, true, sequence != nil && sequence.ctx.Err() == nil)
	}
	for _, publication := range publications {
		publication.resume()
	}
}

// TestStreamManagerResetSkipsBadEntries: a reset whose stream list contains
// un-openable entries (malformed ids) must still open every valid listed
// stream. Previously the re-open loop aborted on the first failed open,
// stranding the later listed streams until the next reset.
func TestStreamManagerResetSkipsBadEntries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Close()

	streamManager := client.streamManager
	openTracker := newStreamOpenTestTracker(streamManager.streamBuffer)

	destinationId := NewId()
	streamIdA := NewId()
	streamIdB := NewId()

	valid := func(streamId Id) *protocol.StreamOpen {
		return &protocol.StreamOpen{
			StreamId:      streamId.Bytes(),
			DestinationId: destinationId.Bytes(),
		}
	}

	frame, err := ToFrame(&protocol.StreamReset{
		Streams: []*protocol.StreamOpen{
			// malformed: a truncated stream id fails IdFromBytes
			{
				StreamId:      []byte{0x01, 0x02, 0x03},
				DestinationId: destinationId.Bytes(),
			},
			valid(streamIdA),
			// malformed: a truncated source id fails IdFromBytes
			{
				StreamId: NewId().Bytes(),
				SourceId: []byte{0x04},
			},
			valid(streamIdB),
		},
	}, DefaultProtocolVersion)
	AssertEqual(t, err, nil)
	publicationA := openTracker.expectPublication(streamIdA)
	publicationB := openTracker.expectPublication(streamIdB)
	streamManager.Receive(SourceId(ControlId), []*protocol.Frame{frame}, Peer{})
	publicationA.wait(t)
	publicationB.wait(t)

	// both valid streams opened despite the malformed entries around them
	AssertEqual(t, true, streamManager.IsStreamOpen(streamIdA))
	AssertEqual(t, true, streamManager.IsStreamOpen(streamIdB))
	publicationA.resume()
	publicationB.resume()
}

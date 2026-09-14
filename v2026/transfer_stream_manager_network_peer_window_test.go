package connect

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// A selected network peer reaches its provider through ephemeral per-window
// clients, while the platform's peer list names only top-level device clients
// (`AddNetworkPeer` is guarded by `topLevel`, and
// `topLevel = sourceClientId == nil`). Resolving provider stream policy
// against that list alone can therefore never admit a real network-peer P2P
// stream.
//
// Found on two physical Android devices on 2026-07-28 and recorded in
// OPTIMIZENETWORKPEER1.md §10: the provider logged one
// `reject disabled provider ... destination=<window client>` and then served
// that same peer's authenticated Network contracts 295 ms later, while the
// client rebuilt a doomed PeerConnection every 15 s for the whole session.
//
// The witness for the missing identity is a contract that verifies against
// this client's own `ProvideMode_Network` secret key.

func streamManagerNetworkOnlyProviderClient(ctx context.Context) *Client {
	settings := DefaultClientSettings()
	settings.ProviderStreamPolicy = true
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	// Network-only provider: no blanket Public/FriendsAndFamily allowance.
	// This is also the effective policy of a public provider whose providing
	// is paused, because `allowAny` is gated on the pause while `allowNetwork`
	// deliberately is not.
	client.ContractManager().SetProvideModesWithReturnTraffic(
		map[protocol.ProvideMode]bool{
			protocol.ProvideMode_Network: true,
		},
	)
	return client
}

func streamManagerMustFrame(t *testing.T, message proto.Message) *protocol.Frame {
	frame, err := ToFrame(message, DefaultProtocolVersion)
	AssertEqual(t, err, nil)
	return frame
}

func streamManagerAnnouncePeer(t *testing.T, client *Client, peerDeviceId Id) {
	client.receive(
		SourceId(ControlId),
		[]*protocol.Frame{
			streamManagerMustFrame(t, &protocol.NetworkPeersReset{}),
			streamManagerMustFrame(t, &protocol.NetworkPeersUpdate{
				Peers: []*protocol.NetworkPeer{{
					ClientId:     peerDeviceId.Bytes(),
					ProvideModes: []protocol.ProvideMode{protocol.ProvideMode_Network},
				}},
			}),
		},
		Peer{},
	)
}

func streamManagerOpenStream(t *testing.T, client *Client, destinationId Id, streamId Id) {
	client.receive(
		SourceId(ControlId),
		[]*protocol.Frame{
			streamManagerMustFrame(t, &protocol.StreamOpen{
				DestinationId: destinationId.Bytes(),
				StreamId:      streamId.Bytes(),
			}),
		},
		Peer{},
	)
}

// streamManagerVerifiedContract builds a contract that genuinely verifies
// against the client's own provider secret key for the given mode. This is
// the exact proof `registerContracts` checks before it reports a window
// client as an authenticated same-network endpoint.
func streamManagerVerifiedContract(
	t *testing.T,
	client *Client,
	relationship protocol.ProvideMode,
	sourceId Id,
	streamId Id,
) *protocol.Contract {
	contractManager := client.ContractManager()
	provideSecretKey, ok := contractManager.GetProvideSecretKey(relationship)
	AssertEqual(t, true, ok)

	storedContract := &protocol.StoredContract{
		ContractId:        NewId().Bytes(),
		TransferByteCount: uint64(mib(1)),
		SourceId:          sourceId.Bytes(),
		DestinationId:     client.ClientId().Bytes(),
		StreamId:          streamId.Bytes(),
	}
	storedContractBytes, err := ProtoMarshal(storedContract)
	AssertEqual(t, nil, err)

	return &protocol.Contract{
		StoredContractBytes: storedContractBytes,
		StoredContractHmac: SignStoredContract(
			contractManager.settings,
			provideSecretKey,
			storedContractBytes,
		),
		ProvideMode: relationship,
	}
}

// TestStreamManagerVerifiedNetworkContractAdmitsWindowClientStream is the
// regression for the physical failure: a StreamOpen naming an ephemeral
// window client is refused while unproven, and admitted once that exact id
// presents a contract verified against the provider's Network secret key.
func TestStreamManagerVerifiedNetworkContractAdmitsWindowClientStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := streamManagerNetworkOnlyProviderClient(ctx)
	defer client.Close()
	openTracker := newStreamOpenTestTracker(client.streamManager.streamBuffer)

	peerDeviceId := NewId()
	streamManagerAnnouncePeer(t, client, peerDeviceId)

	// The device's ephemeral window client: structurally absent from every
	// peer list, and the identity every real network-peer P2P stream carries.
	windowClientId := NewId()
	streamId := NewId()
	streamManagerOpenStream(t, client, windowClientId, streamId)
	if client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("window client stream was admitted before any proof of relationship")
	}

	// The proof arrives, as it physically does, after the StreamOpen.
	publication := openTracker.expectPublication(streamId)
	client.streamManager.NetworkPeerWindowClientAuthenticated(windowClientId)
	publication.wait(t)
	if !client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("window client stream was not admitted after its Network contract verified")
	}
	publication.resume()
}

// TestStreamManagerUnprovenWindowClientStreamIsRefused pins that the fix did
// not weaken the boundary: an id that never presented a verifying Network
// contract is still refused by a Network-only provider, even while an
// unrelated peer is announced and another window client is proven.
func TestStreamManagerUnprovenWindowClientStreamIsRefused(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := streamManagerNetworkOnlyProviderClient(ctx)
	defer client.Close()

	streamManagerAnnouncePeer(t, client, NewId())
	client.streamManager.NetworkPeerWindowClientAuthenticated(NewId())

	strangerId := NewId()
	streamId := NewId()
	streamManagerOpenStream(t, client, strangerId, streamId)

	if client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("an unproven window client opened a stream on a network-only provider")
	}

	// Proving a different id must not admit it either.
	client.streamManager.NetworkPeerWindowClientAuthenticated(NewId())
	if client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("proving an unrelated id admitted the stranger's stream")
	}
}

// TestStreamManagerRegisterContractsAuthenticatesOnlyNetworkMode drives the
// real receive path. A verified Network contract must authenticate its
// sender; a verified Public contract must not, because Public providing is a
// different relationship and does not place the sender in this network.
func TestStreamManagerRegisterContractsAuthenticatesOnlyNetworkMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.ProviderStreamPolicy = true
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Close()
	client.ContractManager().SetProvideModesWithReturnTraffic(
		map[protocol.ProvideMode]bool{
			protocol.ProvideMode_Network: true,
			protocol.ProvideMode_Public:  true,
		},
	)

	registerContract := func(relationship protocol.ProvideMode, sourceId Id) error {
		streamId := NewId()
		contract := streamManagerVerifiedContract(t, client, relationship, sourceId, streamId)
		contractFrame, err := ToFrame(contract, DefaultProtocolVersion)
		AssertEqual(t, nil, err)

		receiveSequence := &ReceiveSequence{
			ctx:                   ctx,
			client:                client,
			log:                   client.settings.Log,
			source:                TransferPath{SourceId: sourceId},
			receiveBufferSettings: client.settings.ReceiveBufferSettings,
			openReceiveContracts:  map[Id]*sequenceContract{},
			peerAudit:             NewSequencePeerAudit(client, TransferPath{SourceId: sourceId}, 0),
		}
		return receiveSequence.registerContracts(&receiveItem{contractFrame: contractFrame})
	}

	networkSourceId := NewId()
	AssertEqual(t, nil, registerContract(protocol.ProvideMode_Network, networkSourceId))
	if !client.peerManager.isNetworkPeer(networkSourceId) {
		t.Fatal("a verified Network contract did not authenticate its sender")
	}

	publicSourceId := NewId()
	AssertEqual(t, nil, registerContract(protocol.ProvideMode_Public, publicSourceId))
	if client.peerManager.isNetworkPeer(publicSourceId) {
		t.Fatal("a verified Public contract authenticated its sender as a network peer")
	}
}

// TestStreamManagerRejectedStreamOpensAreBounded pins that the retained
// refusals cannot grow without bound on remote control input, and that the
// newest refusal survives eviction so a live peer is still reconsidered.
func TestStreamManagerRejectedStreamOpensAreBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := streamManagerNetworkOnlyProviderClient(ctx)
	defer client.Close()
	openTracker := newStreamOpenTestTracker(client.streamManager.streamBuffer)

	var lastWindowClientId Id
	var lastStreamId Id
	for i := range 4 * maxRejectedStreamOpens {
		windowClientId := NewId()
		streamId := NewId()
		streamManagerOpenStream(t, client, windowClientId, streamId)
		if i == 4*maxRejectedStreamOpens-1 {
			lastWindowClientId = windowClientId
			lastStreamId = streamId
		}
	}

	client.streamManager.rejectedStreamsLock.Lock()
	retained := len(client.streamManager.rejectedStreams)
	client.streamManager.rejectedStreamsLock.Unlock()
	if maxRejectedStreamOpens < retained {
		t.Fatalf("retained %d refused stream opens, want at most %d", retained, maxRejectedStreamOpens)
	}

	publication := openTracker.expectPublication(lastStreamId)
	client.streamManager.NetworkPeerWindowClientAuthenticated(lastWindowClientId)
	publication.wait(t)
	if !client.streamManager.IsStreamOpen(lastStreamId) {
		t.Fatal("the newest refusal was evicted and could not be reconsidered")
	}
	publication.resume()
}

// TestStreamManagerNetworkPeerWindowClientsAreBounded pins the witness map
// against a peer that churns window client identities.
func TestStreamManagerNetworkPeerWindowClientsAreBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := streamManagerNetworkOnlyProviderClient(ctx)
	defer client.Close()

	maxCount := client.settings.PeerManagerSettings.MaxNetworkPeerWindowClients
	var newestId Id
	for i := range 4 * maxCount {
		clientId := NewId()
		client.streamManager.NetworkPeerWindowClientAuthenticated(clientId)
		if i == 4*maxCount-1 {
			newestId = clientId
		}
	}

	client.peerManager.stateLock.Lock()
	retained := len(client.peerManager.networkPeerWindowClients)
	client.peerManager.stateLock.Unlock()
	if maxCount < retained {
		t.Fatalf("retained %d window client witnesses, want at most %d", retained, maxCount)
	}
	if !client.peerManager.isNetworkPeer(newestId) {
		t.Fatal("the most recently proven window client was evicted")
	}
}

// TestStreamManagerClosedStreamIsNotResurrected pins that a hop the platform
// retired cannot be reopened by a later proof.
func TestStreamManagerClosedStreamIsNotResurrected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := streamManagerNetworkOnlyProviderClient(ctx)
	defer client.Close()

	windowClientId := NewId()
	streamId := NewId()
	streamManagerOpenStream(t, client, windowClientId, streamId)

	client.receive(
		SourceId(ControlId),
		[]*protocol.Frame{
			streamManagerMustFrame(t, &protocol.StreamClose{StreamId: streamId.Bytes()}),
		},
		Peer{},
	)

	client.streamManager.NetworkPeerWindowClientAuthenticated(windowClientId)
	if client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("a platform-retired stream was resurrected by a later proof")
	}
}

// TestStreamManagerProvenWindowClientStreamSurvivesPeerReconcile pins the
// admission and retirement predicates as the same one. A peer membership
// update reconciles open provider streams, and resolving that against
// announced top-level peers alone would immediately close the window client
// stream that admission correctly allowed.
func TestStreamManagerProvenWindowClientStreamSurvivesPeerReconcile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := streamManagerNetworkOnlyProviderClient(ctx)
	defer client.Close()
	openTracker := newStreamOpenTestTracker(client.streamManager.streamBuffer)

	windowClientId := NewId()
	streamId := NewId()
	streamManagerOpenStream(t, client, windowClientId, streamId)
	publication := openTracker.expectPublication(streamId)
	client.streamManager.NetworkPeerWindowClientAuthenticated(windowClientId)
	publication.wait(t)
	if !client.streamManager.IsStreamOpen(streamId) {
		t.Fatal("proven window client stream was not admitted")
	}
	sequence := streamLifecycleSequence(client, streamId)

	// An unrelated peer membership change reconciles provider streams.
	streamManagerAnnouncePeer(t, client, NewId())
	if !client.streamManager.IsStreamOpen(streamId) || sequence == nil || sequence.ctx.Err() != nil {
		t.Fatal("peer reconcile retired a proven window client stream")
	}
	publication.resume()
}

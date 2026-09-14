package connect

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"testing"
	"time"

	// mathrand "math/rand"

	// "google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestTakeContractStreamStamped pins the oob loop behind platform stream
// steering: the reply sequence requests contracts under a plain (no-stream)
// contract key, and the platform may return a contract whose stored bytes
// carry a stream id ("switch to the stream"). The stamped contract must
// queue under the request's key and carry the stream id through
// `newSequenceContract` into the sequence path — which is what steers the
// writer onto the stream and marks the contract stats.
func TestTakeContractStreamStamped(t *testing.T) {
	ctx := context.Background()
	clientId := NewId()
	settings := DefaultClientSettings()
	settings.ContractManagerSettings.LegacyCreateContract = true
	client := NewClient(ctx, clientId, NewNoContractClientOob(), settings)
	defer client.Cancel()
	contractManager := client.ContractManager()

	destinationId := NewId()
	streamId := NewId()

	contractManager.SetProvideModesWithReturnTraffic(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
	})
	relationship := protocol.ProvideMode_Network
	provideSecretKey, ok := contractManager.GetProvideSecretKey(relationship)
	AssertEqual(t, true, ok)

	storedContract := &protocol.StoredContract{
		ContractId:        NewId().Bytes(),
		TransferByteCount: uint64(gib(1)),
		SourceId:          clientId.Bytes(),
		DestinationId:     destinationId.Bytes(),
		StreamId:          streamId.Bytes(),
	}
	storedContractBytes, err := ProtoMarshal(storedContract)
	AssertEqual(t, nil, err)
	storedContractHmac := SignStoredContract(contractManager.settings, provideSecretKey, storedContractBytes)

	result := &protocol.CreateContractResult{
		Contract: &protocol.Contract{
			StoredContractBytes: storedContractBytes,
			StoredContractHmac:  storedContractHmac,
			ProvideMode:         relationship,
		},
	}
	frame, err := ToFrame(result, DefaultProtocolVersion)
	AssertEqual(t, nil, err)

	// the request key has no stream — the requester does not know the
	// platform will steer it
	contractKey := ContractKey{
		Destination: DestinationId(destinationId),
	}
	err = contractManager.HandleControlFrame(contractKey, frame)
	AssertEqual(t, nil, err)

	contract := contractManager.TakeContract(ctx, contractKey, 5*time.Second)
	AssertEqual(t, true, contract != nil)

	sequenceContract, err := newSequenceContract(client.log, "s", contract, 1, 1.0)
	AssertEqual(t, nil, err)
	AssertEqual(t, true, sequenceContract.path.IsStream())
	AssertEqual(t, streamId, sequenceContract.path.StreamId)
	AssertEqual(
		t,
		TransferPath{DestinationId: destinationId, StreamId: streamId},
		sequenceContract.path.DestinationMask(),
	)
}

func TestInitialNetworkPeerContractCoversColdPageBurst(t *testing.T) {
	settings := DefaultContractManagerSettings()
	manager := &ContractManager{settings: settings}

	byteCount := manager.contractByteCount(
		ContractKey{Destination: DestinationId(NewId()), NetworkPeer: true},
		0,
		0,
	)
	AssertEqual(t, mib(1), byteCount)
}

// the opening contract is the configured initial size for every sequence
// class -- see the InitialContractTransferByteCount rationale: the opening
// size trades pre-settlement escrow exposure against the blocking
// negotiation every new destination pays at sequence 0
func TestInitialPublicForceStreamContractOpensAtInitialSize(t *testing.T) {
	settings := DefaultContractManagerSettings()
	manager := &ContractManager{settings: settings}

	byteCount := manager.contractByteCount(ContractKey{ForceStream: true}, 0, 0)
	AssertEqual(t, settings.InitialContractTransferByteCount, byteCount)
}

func TestInitialRegularContractOpensAtInitialSize(t *testing.T) {
	settings := DefaultContractManagerSettings()
	manager := &ContractManager{settings: settings}

	byteCount := manager.contractByteCount(ContractKey{}, 0, 0)
	AssertEqual(t, settings.InitialContractTransferByteCount, byteCount)
}

func TestConnectedNetworkPeerUsesLargeInitialContract(t *testing.T) {
	peerId := NewId()
	peerManager := NewPeerManager(
		context.Background(),
		nil,
		DefaultPeerManagerSettings(),
	)
	_, err := peerManager.updatePeers(&protocol.NetworkPeersUpdate{
		Peers: []*protocol.NetworkPeer{{
			ClientId: peerId.Bytes(),
		}},
	})
	AssertEqual(t, err, nil)
	manager := &ContractManager{
		client:   &Client{peerManager: peerManager},
		settings: DefaultContractManagerSettings(),
	}

	byteCount := manager.contractByteCount(
		ContractKey{Destination: DestinationId(peerId), ForceStream: false},
		0,
		0,
	)
	AssertEqual(t, mib(1), byteCount)
}

func TestDisconnectedNetworkPeerKeepsInitialContract(t *testing.T) {
	peerId := NewId()
	disconnectTime := uint64(time.Now().UnixMilli())
	peerManager := NewPeerManager(
		context.Background(),
		nil,
		DefaultPeerManagerSettings(),
	)
	_, err := peerManager.updatePeers(&protocol.NetworkPeersUpdate{
		Peers: []*protocol.NetworkPeer{{
			ClientId:       peerId.Bytes(),
			DisconnectTime: &disconnectTime,
		}},
	})
	AssertEqual(t, err, nil)
	manager := &ContractManager{
		client:   &Client{peerManager: peerManager},
		settings: DefaultContractManagerSettings(),
	}

	byteCount := manager.contractByteCount(
		ContractKey{Destination: DestinationId(peerId), ForceStream: true},
		0,
		0,
	)
	AssertEqual(t, DefaultContractManagerSettings().InitialContractTransferByteCount, byteCount)
}

func TestInitialNetworkPeerContractHonorsLargerMessageMinimum(t *testing.T) {
	settings := DefaultContractManagerSettings()
	manager := &ContractManager{settings: settings}

	byteCount := manager.contractByteCount(
		ContractKey{Destination: DestinationId(NewId()), NetworkPeer: true},
		0,
		mib(2),
	)
	AssertEqual(t, mib(2), byteCount)
}

func TestInitialNetworkPeerContractDoesNotExceedStandardBound(t *testing.T) {
	settings := DefaultContractManagerSettings()
	settings.InitialNetworkPeerContractTransferByteCount = settings.StandardContractTransferByteCount * 2
	manager := &ContractManager{settings: settings}

	byteCount := manager.contractByteCount(
		ContractKey{Destination: DestinationId(NewId()), NetworkPeer: true},
		0,
		0,
	)
	AssertEqual(t, settings.StandardContractTransferByteCount, byteCount)
}

func TestInitialNetworkPeerContractSupportsPartialSettings(t *testing.T) {
	settings := &ContractManagerSettings{
		InitialContractTransferByteCount:            kib(16),
		InitialNetworkPeerContractTransferByteCount: mib(1),
	}
	manager := &ContractManager{settings: settings}

	byteCount := manager.contractByteCount(
		ContractKey{Destination: DestinationId(NewId()), NetworkPeer: true},
		0,
		0,
	)
	AssertEqual(t, mib(1), byteCount)
}

func TestContractByteCountDoesNotWrapNegativeSettings(t *testing.T) {
	settings := &ContractManagerSettings{
		InitialContractTransferByteCount:            -1,
		InitialNetworkPeerContractTransferByteCount: -1,
		StandardContractTransferByteCount:           -1,
	}
	manager := &ContractManager{settings: settings}

	byteCount := manager.contractByteCount(
		ContractKey{ForceStream: true},
		0,
		-1,
	)
	AssertEqual(t, ByteCount(0), byteCount)
}

func TestNetworkPeerContractRampReachesStandardBound(t *testing.T) {
	settings := DefaultContractManagerSettings()
	manager := &ContractManager{settings: settings}
	contractKey := ContractKey{
		Destination: DestinationId(NewId()),
		NetworkPeer: true,
	}

	previousByteCount := ByteCount(0)
	for contractSeqIndex := uint64(0); contractSeqIndex <= settings.ContractTransferByteSeqScale; contractSeqIndex += 1 {
		byteCount := manager.contractByteCount(contractKey, contractSeqIndex, 0)
		if byteCount < previousByteCount {
			t.Fatalf(
				"contract %d byte count decreased from %d to %d",
				contractSeqIndex,
				previousByteCount,
				byteCount,
			)
		}
		previousByteCount = byteCount
	}
	AssertEqual(t, settings.StandardContractTransferByteCount, previousByteCount)
}

func TestTakeContract(t *testing.T) {
	// in parallel, add contracts, take contracts, and optionally return contract
	// make sure all created contracts get eventually taken

	k := 4
	n := 64
	// contractReturnP := float32(0.5)
	timeout := 30 * time.Second

	ctx := context.Background()
	clientId := NewId()
	settings := DefaultClientSettings()
	settings.ContractManagerSettings.LegacyCreateContract = true
	client := NewClient(ctx, clientId, NewNoContractClientOob(), settings)
	defer client.Cancel()
	contractManager := client.ContractManager()

	destinationId := NewId()

	contractManager.SetProvideModesWithReturnTraffic(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
		protocol.ProvideMode_Public:  true,
	})

	contracts := make(chan *protocol.Contract)
	contractTimeout := make(chan struct{}, 1)

	go func() {
		for i := 0; i < k*n; i += 1 {
			contractId := NewId()
			contractByteCount := gib(1)

			relationship := protocol.ProvideMode_Public
			provideSecretKey, ok := contractManager.GetProvideSecretKey(relationship)
			AssertEqual(t, true, ok)

			storedContract := &protocol.StoredContract{
				ContractId:        contractId.Bytes(),
				TransferByteCount: uint64(contractByteCount),
				SourceId:          clientId.Bytes(),
				DestinationId:     destinationId.Bytes(),
			}
			storedContractBytes, err := ProtoMarshal(storedContract)
			AssertEqual(t, nil, err)
			defer MessagePoolReturn(storedContractBytes)
			storedContractHmac := SignStoredContract(contractManager.settings, provideSecretKey, storedContractBytes)

			verified := contractManager.Verify(storedContractHmac, storedContractBytes, relationship)
			AssertEqual(t, true, verified)

			result := &protocol.CreateContractResult{
				Contract: &protocol.Contract{
					StoredContractBytes: storedContractBytes,
					StoredContractHmac:  storedContractHmac,
					ProvideMode:         relationship,
				},
			}
			frame, err := ToFrame(result, DefaultProtocolVersion)
			AssertEqual(t, nil, err)

			contractManager.HandleControlFrame(
				ContractKey{
					Destination: DestinationId(destinationId),
				},
				frame,
			)
		}
	}()

	for j := 0; j < k; j += 1 {
		go func() {
			for i := 0; i < n; {

				contractKey := ContractKey{
					Destination: DestinationId(destinationId),
				}
				if contract := contractManager.TakeContract(ctx, contractKey, timeout); contract != nil {
					// if mathrand.Float32() < contractReturnP {
					// 	// put back
					// 	contractManager.ReturnContract(ctx, destinationId, contract)
					// } else {
					select {
					case contracts <- contract:
					case <-time.After(timeout):
						select {
						case contractTimeout <- struct{}{}:
						default:
						}
						return
					}
					i += 1
					// }
				}

			}

		}()
	}

	contractIds := map[Id]bool{}

	for i := 0; i < k*n; i += 1 {
		select {
		case contract := <-contracts:
			var storedContract protocol.StoredContract
			err := ProtoUnmarshal(contract.StoredContractBytes, &storedContract)
			AssertEqual(t, nil, err)

			contractId, err := IdFromBytes(storedContract.ContractId)
			AssertEqual(t, nil, err)

			AssertEqual(t, false, contractIds[contractId])
			contractIds[contractId] = true

		case <-time.After(timeout):
			t.FailNow()
		case <-contractTimeout:
			t.FailNow()
		}
	}

	AssertEqual(t, k*n, len(contractIds))

	// no more
	contractKey := ContractKey{
		Destination: DestinationId(destinationId),
	}
	contract := contractManager.TakeContract(ctx, contractKey, 0)
	AssertEqual(t, nil, contract)

	// all the contracts are accounted for
}

// TestStoredContractHmacCutover exercises the NetworkEventTimeChangeHmac
// cutover by setting an artificial cutoff time in ContractManagerSettings and
// asserting:
//   - SignStoredContract emits the legacy format when the cutoff is in the future
//   - SignStoredContract emits the standard format when the cutoff is in the past
//   - VerifyStoredContract accepts BOTH formats regardless of the cutoff time
//   - Tampered bytes and wrong keys are rejected for both formats
func TestStoredContractHmacCutover(t *testing.T) {
	provideSecretKey := []byte("test-provide-secret-key-which-is-long-enough")
	storedContractBytes := []byte("test stored contract bytes payload")

	pastSettings := DefaultContractManagerSettings()
	pastSettings.NetworkEventTimeChangeHmac = time.Now().Add(-time.Hour)

	futureSettings := DefaultContractManagerSettings()
	futureSettings.NetworkEventTimeChangeHmac = time.Now().Add(time.Hour)

	// canonical encodings of both formats computed independently of the helper
	legacyExpected := func() []byte {
		mac := hmac.New(sha256.New, provideSecretKey)
		return mac.Sum(storedContractBytes)
	}()
	standardExpected := func() []byte {
		mac := hmac.New(sha256.New, provideSecretKey)
		mac.Write(storedContractBytes)
		return mac.Sum(nil)
	}()

	// sanity: the two formats have different lengths and contents
	AssertEqual(t, len(storedContractBytes)+sha256.Size, len(legacyExpected))
	AssertEqual(t, sha256.Size, len(standardExpected))

	// future cutoff → signer emits legacy
	futureHmac := SignStoredContract(futureSettings, provideSecretKey, storedContractBytes)
	AssertEqual(t, legacyExpected, futureHmac)

	// past cutoff → signer emits standard
	pastHmac := SignStoredContract(pastSettings, provideSecretKey, storedContractBytes)
	AssertEqual(t, standardExpected, pastHmac)

	// VerifyStoredContract accepts both formats regardless of the cutoff time
	AssertEqual(t, true, VerifyStoredContract(pastSettings, provideSecretKey, storedContractBytes, legacyExpected))
	AssertEqual(t, true, VerifyStoredContract(pastSettings, provideSecretKey, storedContractBytes, standardExpected))
	AssertEqual(t, true, VerifyStoredContract(futureSettings, provideSecretKey, storedContractBytes, legacyExpected))
	AssertEqual(t, true, VerifyStoredContract(futureSettings, provideSecretKey, storedContractBytes, standardExpected))

	// tampered contract bytes are rejected for both formats and both settings
	tampered := []byte("tampered stored contract bytes payload")
	AssertEqual(t, false, VerifyStoredContract(pastSettings, provideSecretKey, tampered, legacyExpected))
	AssertEqual(t, false, VerifyStoredContract(pastSettings, provideSecretKey, tampered, standardExpected))
	AssertEqual(t, false, VerifyStoredContract(futureSettings, provideSecretKey, tampered, legacyExpected))
	AssertEqual(t, false, VerifyStoredContract(futureSettings, provideSecretKey, tampered, standardExpected))

	// wrong provide key is rejected for both formats and both settings
	wrongKey := []byte("wrong-provide-secret-key-which-is-long-enough")
	AssertEqual(t, false, VerifyStoredContract(pastSettings, wrongKey, storedContractBytes, legacyExpected))
	AssertEqual(t, false, VerifyStoredContract(pastSettings, wrongKey, storedContractBytes, standardExpected))
	AssertEqual(t, false, VerifyStoredContract(futureSettings, wrongKey, storedContractBytes, legacyExpected))
	AssertEqual(t, false, VerifyStoredContract(futureSettings, wrongKey, storedContractBytes, standardExpected))

	// an HMAC of an unsupported length is rejected
	bogus := []byte("not-a-valid-hmac")
	AssertEqual(t, false, VerifyStoredContract(pastSettings, provideSecretKey, storedContractBytes, bogus))
}

// TestContractQueueExpire verifies that queued contracts no sequence takes are
// expired: the janitor closes them and removes the emptied queue from
// `destinationContracts` (orphan retention), and `Poll` never hands out a
// contract older than the expire window.
func TestContractQueueExpire(t *testing.T) {
	ctx := context.Background()
	clientId := NewId()
	settings := DefaultClientSettings()
	settings.ContractManagerSettings.LegacyCreateContract = true
	settings.ContractManagerSettings.ContractQueueExpireTimeout = 500 * time.Millisecond
	client := NewClient(ctx, clientId, NewNoContractClientOob(), settings)
	defer client.Cancel()
	contractManager := client.ContractManager()

	destinationId := NewId()

	contractManager.SetProvideModesWithReturnTraffic(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
		protocol.ProvideMode_Public:  true,
	})

	makeContract := func() (*protocol.Contract, *protocol.StoredContract) {
		contractId := NewId()
		relationship := protocol.ProvideMode_Public
		provideSecretKey, ok := contractManager.GetProvideSecretKey(relationship)
		AssertEqual(t, true, ok)

		storedContract := &protocol.StoredContract{
			ContractId:        contractId.Bytes(),
			TransferByteCount: uint64(gib(1)),
			SourceId:          clientId.Bytes(),
			DestinationId:     destinationId.Bytes(),
		}
		storedContractBytes, err := ProtoMarshal(storedContract)
		AssertEqual(t, nil, err)
		storedContractHmac := SignStoredContract(contractManager.settings, provideSecretKey, storedContractBytes)
		contract := &protocol.Contract{
			StoredContractBytes: storedContractBytes,
			StoredContractHmac:  storedContractHmac,
			ProvideMode:         relationship,
		}
		return contract, storedContract
	}

	contractKey := ContractKey{
		Destination: DestinationId(destinationId),
	}

	// queue an orphan via the control frame path (no sequence ever takes it)
	contract, _ := makeContract()
	result := &protocol.CreateContractResult{
		Contract: contract,
	}
	frame, err := ToFrame(result, DefaultProtocolVersion)
	AssertEqual(t, nil, err)
	err = contractManager.HandleControlFrame(contractKey, frame)
	AssertEqual(t, nil, err)

	queueCount := func() int {
		contractManager.mutex.Lock()
		defer contractManager.mutex.Unlock()
		return len(contractManager.destinationContracts)
	}
	AssertEqual(t, 1, queueCount())

	// the janitor expires the orphan and removes the emptied queue
	expired := false
	for range 50 {
		if queueCount() == 0 {
			expired = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	AssertEqual(t, true, expired)

	// the expired contract is no longer takeable
	takenContract := contractManager.TakeContract(ctx, contractKey, 0)
	AssertEqual(t, nil, takenContract)

	// Poll guard: a stale queued contract is never handed out
	queue := newContractQueue(nil, false)
	staleContract, staleStoredContract := makeContract()
	queue.Add(staleContract, staleStoredContract)
	polled, expiredContracts := queue.Poll(time.Now().Add(time.Minute))
	AssertEqual(t, nil, polled)
	AssertEqual(t, 1, len(expiredContracts))

	// a fresh contract polled with expiry disabled (zero minEnqueueTime) is handed out
	freshContract, freshStoredContract := makeContract()
	queue.Add(freshContract, freshStoredContract)
	polled, expiredContracts = queue.Poll(time.Time{})
	AssertEqual(t, freshContract, polled)
	AssertEqual(t, 0, len(expiredContracts))
}

func newActiveContractPrefetchTestManager(
	t *testing.T,
	networkPeer bool,
) (*ContractManager, ContractKey, *protocol.Contract) {
	t.Helper()

	destinationId := NewId()
	contractKey := ContractKey{
		Destination: DestinationId(destinationId),
		NetworkPeer: networkPeer,
	}
	prefetchId := NewId()
	storedContract := &protocol.StoredContract{
		ContractId:        prefetchId.Bytes(),
		TransferByteCount: uint64(mib(1)),
		SourceId:          NewId().Bytes(),
		DestinationId:     destinationId.Bytes(),
	}
	storedContractBytes, err := ProtoMarshal(storedContract)
	AssertEqual(t, err, nil)
	prefetch := &protocol.Contract{
		StoredContractBytes: storedContractBytes,
	}
	queue := newContractQueue(NewNoopLogger(), false)
	AssertEqual(t, queue.Add(prefetch, storedContract), nil)
	queue.mutex.Lock()
	queue.contracts[prefetchId].enqueueTime = time.Now().Add(-time.Hour)
	queue.mutex.Unlock()

	manager := &ContractManager{
		ctx:                  context.Background(),
		client:               &Client{log: NewNoopLogger()},
		settings:             DefaultContractManagerSettings(),
		destinationContracts: map[ContractKey]*contractQueue{contractKey: queue},
		localStats:           NewContractManagerStats(),
	}
	openContractId := NewId()
	manager.localStats.ContractOpenByteCounts[openContractId] = mib(1)
	manager.localStats.ContractOpenKeys[openContractId] = contractKey
	return manager, contractKey, prefetch
}

func TestContractQueueJanitorKeepsPrefetchForOpenContract(t *testing.T) {
	manager, contractKey, _ := newActiveContractPrefetchTestManager(t, true)

	expired := manager.expireQueuedContractsBefore(time.Now())
	AssertEqual(t, 0, len(expired))

	manager.mutex.Lock()
	queue := manager.destinationContracts[contractKey]
	manager.mutex.Unlock()
	if queue == nil {
		t.Fatal("janitor removed a live sequence's prefetch queue")
	}
	queue.mutex.Lock()
	pendingCount := len(queue.contracts)
	queue.mutex.Unlock()
	AssertEqual(t, 1, pendingCount)
}

func TestContractQueueJanitorBoundsStaleNetworkPrefetches(t *testing.T) {
	manager, contractKey, first := newActiveContractPrefetchTestManager(t, true)
	manager.mutex.Lock()
	queue := manager.destinationContracts[contractKey]
	manager.mutex.Unlock()

	secondId := NewId()
	secondStored := &protocol.StoredContract{
		ContractId:        secondId.Bytes(),
		TransferByteCount: uint64(mib(1)),
		SourceId:          NewId().Bytes(),
		DestinationId:     contractKey.Destination.DestinationId.Bytes(),
	}
	secondBytes, err := ProtoMarshal(secondStored)
	AssertEqual(t, err, nil)
	second := &protocol.Contract{StoredContractBytes: secondBytes}
	AssertEqual(t, queue.Add(second, secondStored), nil)
	queue.mutex.Lock()
	queue.contracts[secondId].enqueueTime = time.Now().Add(-30 * time.Minute)
	queue.mutex.Unlock()

	expired := manager.expireQueuedContractsBefore(time.Now())
	AssertEqual(t, 1, len(expired))
	AssertEqual(t, first, expired[0])

	queue.mutex.Lock()
	pendingCount := len(queue.contracts)
	_, retainedNewest := queue.contracts[secondId]
	queue.mutex.Unlock()
	AssertEqual(t, 1, pendingCount)
	AssertEqual(t, true, retainedNewest)
}

func TestContractQueueJanitorPrefersFreshNetworkPrefetch(t *testing.T) {
	manager, contractKey, stale := newActiveContractPrefetchTestManager(t, true)
	manager.mutex.Lock()
	queue := manager.destinationContracts[contractKey]
	manager.mutex.Unlock()

	freshId := NewId()
	freshStored := &protocol.StoredContract{
		ContractId:        freshId.Bytes(),
		TransferByteCount: uint64(mib(1)),
		SourceId:          NewId().Bytes(),
		DestinationId:     contractKey.Destination.DestinationId.Bytes(),
	}
	freshBytes, err := ProtoMarshal(freshStored)
	AssertEqual(t, err, nil)
	fresh := &protocol.Contract{StoredContractBytes: freshBytes}
	AssertEqual(t, queue.Add(fresh, freshStored), nil)

	expired := manager.expireQueuedContractsBefore(time.Now().Add(-time.Minute))
	AssertEqual(t, 1, len(expired))
	AssertEqual(t, stale, expired[0])

	queue.mutex.Lock()
	pendingCount := len(queue.contracts)
	_, retainedFresh := queue.contracts[freshId]
	queue.mutex.Unlock()
	AssertEqual(t, 1, pendingCount)
	AssertEqual(t, true, retainedFresh)
}

func TestContractQueueJanitorExpiresPrefetchAfterOpenContractCloses(t *testing.T) {
	manager, contractKey, prefetch := newActiveContractPrefetchTestManager(t, true)
	manager.mutex.Lock()
	clear(manager.localStats.ContractOpenByteCounts)
	clear(manager.localStats.ContractOpenKeys)
	manager.mutex.Unlock()

	expired := manager.expireQueuedContractsBefore(time.Now())
	AssertEqual(t, 1, len(expired))
	AssertEqual(t, prefetch, expired[0])

	manager.mutex.Lock()
	_, retained := manager.destinationContracts[contractKey]
	manager.mutex.Unlock()
	if retained {
		t.Fatal("janitor retained an orphaned prefetch queue")
	}
}

func TestContractQueueJanitorExpiresPublicPrefetchWithOpenContract(t *testing.T) {
	manager, contractKey, prefetch := newActiveContractPrefetchTestManager(t, false)

	expired := manager.expireQueuedContractsBefore(time.Now())
	AssertEqual(t, 1, len(expired))
	AssertEqual(t, prefetch, expired[0])

	manager.mutex.Lock()
	_, retained := manager.destinationContracts[contractKey]
	manager.mutex.Unlock()
	if retained {
		t.Fatal("janitor retained an escrow-capable public prefetch")
	}
}

func TestTakeContractKeepsPrefetchForOpenContractPastExpiry(t *testing.T) {
	manager, contractKey, prefetch := newActiveContractPrefetchTestManager(t, true)
	manager.settings.ContractQueueExpireTimeout = time.Millisecond

	taken := manager.TakeContract(context.Background(), contractKey, 0)
	AssertEqual(t, prefetch, taken)
}

func TestResetLocalStatsPreservesOpenContractOwnership(t *testing.T) {
	manager, contractKey, _ := newActiveContractPrefetchTestManager(t, true)

	manager.ResetLocalStats()

	if !manager.hasOpenContractForKey(contractKey) {
		t.Fatal("stats reset discarded live contract ownership")
	}
	stats := manager.LocalStats()
	AssertEqual(t, 1, len(stats.ContractOpenKeys))
	AssertEqual(t, 1, len(stats.ContractOpenByteCounts))
}

func TestContractQueueStaleCloseCannotDeleteReplacementGeneration(t *testing.T) {
	manager := &ContractManager{
		client: &Client{log: NewNoopLogger()},
		settings: &ContractManagerSettings{
			TrackUsedContracts: true,
		},
		destinationContracts: map[ContractKey]*contractQueue{},
	}
	key := ContractKey{Destination: DestinationId(NewId())}

	// One ordinary owner remains blocked on the old queue while a sequence
	// flush opens the same generation and force-removes it.
	oldOwner := manager.openContractQueue(key)
	flusher := manager.openContractQueue(key)
	if flusher != oldOwner {
		t.Fatal("same key did not share the initial queue generation")
	}
	manager.closeContractQueueWithForceRemove(key, flusher, true)
	if !oldOwner.Drained() {
		t.Fatal("force-removed queue did not wake its old waiters")
	}

	replacement := manager.openContractQueue(key)
	if replacement == oldOwner {
		t.Fatal("force removal did not create a new queue generation")
	}

	// The delayed owner must close its own drained queue, not look up the key
	// and decrement/delete the replacement.
	manager.closeContractQueue(key, oldOwner)
	manager.mutex.Lock()
	current := manager.destinationContracts[key]
	manager.mutex.Unlock()
	if current != replacement {
		t.Fatal("stale queue close deleted the replacement generation")
	}
	replacement.mutex.Lock()
	replacementOpenCount := replacement.openCount
	replacement.mutex.Unlock()
	if replacementOpenCount != 1 {
		t.Fatalf("stale close changed replacement open count to %d, want 1", replacementOpenCount)
	}

	manager.closeContractQueue(key, replacement)
	manager.mutex.Lock()
	_, retained := manager.destinationContracts[key]
	manager.mutex.Unlock()
	if retained {
		t.Fatal("current queue generation was not removed after its own close")
	}
}

// A key opener blocked behind retirement must never inherit the drained queue
// generation. Test seams pause retirement while it owns the manager lock and
// prove the competing opener reached that exact boundary.
func TestFlushContractQueueDrainAndDetachAreAtomic(t *testing.T) {
	manager := &ContractManager{
		client: &Client{log: NewNoopLogger()},
		settings: &ContractManagerSettings{
			TrackUsedContracts: true,
		},
		destinationContracts: map[ContractKey]*contractQueue{},
	}
	key := ContractKey{Destination: DestinationId(NewId())}
	oldQueue := manager.openContractQueue(key)
	flushOwnsManager := make(chan struct{})
	releaseFlush := make(chan struct{})
	oldQueue.testingBeforeFlushWithLock = func() {
		close(flushOwnsManager)
		<-releaseFlush
	}

	flushDone := make(chan struct{})
	go func() {
		manager.FlushContractQueue(key, true)
		close(flushDone)
	}()

	waitForStreamLifecycleSignal(
		t,
		flushOwnsManager,
		"flush did not reach the queue retirement boundary",
	)
	if manager.mutex.TryLock() {
		manager.mutex.Unlock()
		close(releaseFlush)
		t.Fatal("flush reached its drain boundary without owning the manager lock")
	}
	openerAttempted := make(chan struct{})
	manager.testingBeforeOpenContractQueueLock = func(openKey ContractKey) {
		if openKey != key {
			t.Errorf("competing opener key=%+v, want %+v", openKey, key)
		}
		close(openerAttempted)
	}
	freshQueueResult := make(chan *contractQueue, 1)
	go func() {
		freshQueueResult <- manager.openContractQueue(key)
	}()
	waitForStreamLifecycleSignal(
		t,
		openerAttempted,
		"competing opener did not reach the retirement boundary",
	)

	close(releaseFlush)
	var freshQueue *contractQueue
	select {
	case freshQueue = <-freshQueueResult:
	case <-time.After(5 * time.Second):
		t.Fatal("fresh queue opener did not resume after retirement")
	}
	select {
	case <-flushDone:
	case <-time.After(5 * time.Second):
		t.Fatal("queue flush did not finish")
	}
	if freshQueue == oldQueue {
		t.Fatal("new opener inherited the drained queue generation")
	}
	if !oldQueue.Drained() {
		t.Fatal("retired queue was not drained")
	}
	if freshQueue.Drained() {
		t.Fatal("replacement queue started drained")
	}

	manager.closeContractQueue(key, oldQueue)
	manager.closeContractQueue(key, freshQueue)
}

// Logical lanes request the same backend contract class, but their local
// queue generations must be independent. One idle lane may flush only its own
// pending work; draining a healthy sibling would reintroduce cross-flow loss.
func TestLogicalLanesUseIndependentContractQueueGenerations(t *testing.T) {
	manager := &ContractManager{
		client: &Client{log: NewNoopLogger()},
		settings: &ContractManagerSettings{
			TrackUsedContracts: true,
		},
		destinationContracts: map[ContractKey]*contractQueue{},
	}
	base := ContractKey{Destination: DestinationId(NewId())}
	laneOneKey := base
	laneOneKey.LogicalLane = 1
	laneTwoKey := base
	laneTwoKey.LogicalLane = 2
	laneOneQueue := manager.openContractQueue(laneOneKey)
	laneTwoQueue := manager.openContractQueue(laneTwoKey)
	if laneOneQueue == laneTwoQueue {
		t.Fatal("distinct logical lanes shared one local contract queue")
	}

	manager.FlushContractQueue(laneOneKey, true)
	if !laneOneQueue.Drained() {
		t.Fatal("flushed logical lane did not drain its queue")
	}
	if laneTwoQueue.Drained() {
		t.Fatal("flushing one logical lane drained its healthy sibling")
	}
	manager.mutex.Lock()
	retainedLaneTwo := manager.destinationContracts[laneTwoKey]
	_, retainedLaneOne := manager.destinationContracts[laneOneKey]
	manager.mutex.Unlock()
	if retainedLaneOne || retainedLaneTwo != laneTwoQueue {
		t.Fatal("logical-lane contract queue indexes were not isolated")
	}
	manager.closeContractQueue(laneTwoKey, laneTwoQueue)
}

// TestContractQueueShutdownFlush verifies that when the contract manager
// closes (client context canceled), still-queued pending contracts are
// flushed and closed rather than abandoned. The expire timeout is set long so
// only the shutdown path can drain the queue.
func TestContractQueueShutdownFlush(t *testing.T) {
	ctx := context.Background()
	clientId := NewId()
	settings := DefaultClientSettings()
	settings.ContractManagerSettings.LegacyCreateContract = true
	settings.ContractManagerSettings.ContractQueueExpireTimeout = 1 * time.Hour
	client := NewClient(ctx, clientId, NewNoContractClientOob(), settings)
	defer client.Cancel()
	contractManager := client.ContractManager()

	destinationId := NewId()

	contractManager.SetProvideModesWithReturnTraffic(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
		protocol.ProvideMode_Public:  true,
	})

	contractId := NewId()
	relationship := protocol.ProvideMode_Public
	provideSecretKey, ok := contractManager.GetProvideSecretKey(relationship)
	AssertEqual(t, true, ok)
	storedContract := &protocol.StoredContract{
		ContractId:        contractId.Bytes(),
		TransferByteCount: uint64(gib(1)),
		SourceId:          clientId.Bytes(),
		DestinationId:     destinationId.Bytes(),
	}
	storedContractBytes, err := ProtoMarshal(storedContract)
	AssertEqual(t, nil, err)
	storedContractHmac := SignStoredContract(contractManager.settings, provideSecretKey, storedContractBytes)
	contract := &protocol.Contract{
		StoredContractBytes: storedContractBytes,
		StoredContractHmac:  storedContractHmac,
		ProvideMode:         relationship,
	}

	contractKey := ContractKey{
		Destination: DestinationId(destinationId),
	}
	result := &protocol.CreateContractResult{
		Contract: contract,
	}
	frame, err := ToFrame(result, DefaultProtocolVersion)
	AssertEqual(t, nil, err)
	err = contractManager.HandleControlFrame(contractKey, frame)
	AssertEqual(t, nil, err)

	queueCount := func() int {
		contractManager.mutex.Lock()
		defer contractManager.mutex.Unlock()
		return len(contractManager.destinationContracts)
	}
	AssertEqual(t, 1, queueCount())

	// closing the client triggers the shutdown flush of pending contracts
	client.Cancel()

	flushed := false
	for range 50 {
		if queueCount() == 0 {
			flushed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	AssertEqual(t, true, flushed)
}

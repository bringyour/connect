package connect

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestPeerManagerPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerManager := NewPeerManager(ctx, nil, DefaultPeerManagerSettings())

	controlSource := SourceId(ControlId)

	clientIdA := NewId()
	clientIdB := NewId()

	resetFrame := RequireToFrameWithDefaultProtocolVersion(&protocol.NetworkPeersReset{})
	updateFrame := RequireToFrameWithDefaultProtocolVersion(&protocol.NetworkPeersUpdate{
		Peers: []*protocol.NetworkPeer{
			{
				ClientId:     clientIdA.Bytes(),
				ProvideModes: []protocol.ProvideMode{protocol.ProvideMode_Network, protocol.ProvideMode_Stream},
				Principal:    "svc-a",
				Roles:        []string{"role1", "role2"},
				DeviceName:   "device a",
				DeviceSpec:   "spec a",
			},
			{
				ClientId:     clientIdB.Bytes(),
				ProvideModes: []protocol.ProvideMode{protocol.ProvideMode_Stream},
			},
		},
	})

	// frames from a non-control source are ignored
	peerManager.Receive(SourceId(NewId()), []*protocol.Frame{resetFrame, updateFrame}, Peer{ProvideMode: protocol.ProvideMode_Network})
	connected, disconnectedCount := peerManager.NetworkPeers()
	AssertEqual(t, len(connected), 0)
	AssertEqual(t, disconnectedCount, 0)

	notify := peerManager.PeersMonitor().NotifyChannel()

	peerManager.Receive(controlSource, []*protocol.Frame{resetFrame, updateFrame}, Peer{ProvideMode: protocol.ProvideMode_Network})

	select {
	case <-notify:
	default:
		t.Fatal("peers monitor did not notify")
	}

	connected, disconnectedCount = peerManager.NetworkPeers()
	AssertEqual(t, len(connected), 2)
	AssertEqual(t, disconnectedCount, 0)

	peersByClientId := map[Id]*NetworkPeer{}
	for _, networkPeer := range connected {
		peersByClientId[networkPeer.ClientId] = networkPeer
	}
	peerA := peersByClientId[clientIdA]
	AssertNotEqual(t, peerA, nil)
	AssertEqual(t, peerA.ProvideEnabled, true)
	AssertEqual(t, peerA.ProvideModes, []protocol.ProvideMode{protocol.ProvideMode_Network, protocol.ProvideMode_Stream})
	AssertEqual(t, peerA.Principal, "svc-a")
	AssertEqual(t, peerA.Roles, []string{"role1", "role2"})
	AssertEqual(t, peerA.DeviceName, "device a")
	AssertEqual(t, peerA.DeviceSpec, "spec a")
	peerB := peersByClientId[clientIdB]
	AssertNotEqual(t, peerB, nil)
	AssertEqual(t, peerB.ProvideEnabled, false)
	AssertEqual(t, peerB.Principal, "")
	AssertEqual(t, len(peerB.Roles), 0)

	// a disconnect marker moves the peer from connected to disconnected
	disconnectTime := uint64(time.Now().UnixMilli())
	disconnectFrame := RequireToFrameWithDefaultProtocolVersion(&protocol.NetworkPeersUpdate{
		Peers: []*protocol.NetworkPeer{
			{
				ClientId:       clientIdB.Bytes(),
				DisconnectTime: &disconnectTime,
			},
		},
	})
	peerManager.Receive(controlSource, []*protocol.Frame{disconnectFrame}, Peer{ProvideMode: protocol.ProvideMode_Network})

	connected, disconnectedCount = peerManager.NetworkPeers()
	AssertEqual(t, len(connected), 1)
	AssertEqual(t, disconnectedCount, 1)

	// a reconnect upsert clears the disconnect marker
	reconnectFrame := RequireToFrameWithDefaultProtocolVersion(&protocol.NetworkPeersUpdate{
		Peers: []*protocol.NetworkPeer{
			{
				ClientId:     clientIdB.Bytes(),
				ProvideModes: []protocol.ProvideMode{protocol.ProvideMode_Network, protocol.ProvideMode_Stream},
			},
		},
	})
	peerManager.Receive(controlSource, []*protocol.Frame{reconnectFrame}, Peer{ProvideMode: protocol.ProvideMode_Network})

	connected, disconnectedCount = peerManager.NetworkPeers()
	AssertEqual(t, len(connected), 2)
	AssertEqual(t, disconnectedCount, 0)
	AssertEqual(t, peersByClientId[clientIdB].ProvideEnabled, false)
	for _, networkPeer := range connected {
		if networkPeer.ClientId == clientIdB {
			AssertEqual(t, networkPeer.ProvideEnabled, true)
		}
	}

	// a reset clears all state
	peerManager.Receive(controlSource, []*protocol.Frame{resetFrame}, Peer{ProvideMode: protocol.ProvideMode_Network})

	connected, disconnectedCount = peerManager.NetworkPeers()
	AssertEqual(t, len(connected), 0)
	AssertEqual(t, disconnectedCount, 0)
}

func TestPeerManagerDisconnectedPeerWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultPeerManagerSettings()
	settings.DisconnectedPeerWindow = 50 * time.Millisecond
	peerManager := NewPeerManager(ctx, nil, settings)

	controlSource := SourceId(ControlId)

	disconnectTime := uint64(time.Now().UnixMilli())
	disconnectFrame := RequireToFrameWithDefaultProtocolVersion(&protocol.NetworkPeersUpdate{
		Peers: []*protocol.NetworkPeer{
			{
				ClientId:       NewId().Bytes(),
				DisconnectTime: &disconnectTime,
			},
		},
	})
	peerManager.Receive(controlSource, []*protocol.Frame{disconnectFrame}, Peer{ProvideMode: protocol.ProvideMode_Network})

	_, disconnectedCount := peerManager.NetworkPeers()
	AssertEqual(t, disconnectedCount, 1)

	// the disconnect marker ages out of the window
	select {
	case <-time.After(100 * time.Millisecond):
	}
	_, disconnectedCount = peerManager.NetworkPeers()
	AssertEqual(t, disconnectedCount, 0)
}

func TestSequenceContractPeerIdentity(t *testing.T) {
	storedContract := &protocol.StoredContract{
		ContractId:        NewId().Bytes(),
		TransferByteCount: uint64(1024),
		SourceId:          NewId().Bytes(),
		DestinationId:     NewId().Bytes(),
		Roles:             []string{"role1", "role2"},
		Principal:         "svc-a",
	}
	storedContractBytes, err := ProtoMarshal(storedContract)
	AssertEqual(t, err, nil)

	// network provide mode carries the roles and principal from the stored contract
	networkContract := &protocol.Contract{
		StoredContractBytes: storedContractBytes,
		ProvideMode:         protocol.ProvideMode_Network,
	}
	c, err := newSequenceContract(DefaultLogger(), "t", networkContract, ByteCount(0), 1.0)
	AssertEqual(t, err, nil)
	AssertEqual(t, c.roles, []string{"role1", "role2"})
	AssertEqual(t, c.principal, "svc-a")

	// all other provide modes have nil roles and empty principal
	streamContract := &protocol.Contract{
		StoredContractBytes: storedContractBytes,
		ProvideMode:         protocol.ProvideMode_Stream,
	}
	c, err = newSequenceContract(DefaultLogger(), "t", streamContract, ByteCount(0), 1.0)
	AssertEqual(t, err, nil)
	AssertEqual(t, len(c.roles), 0)
	AssertEqual(t, c.principal, "")
}

func TestProvidePausedKeepsNetwork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClientWithDefaults(ctx, NewId(), NewNoContractClientOob())
	defer client.Close()
	contractManager := client.ContractManager()

	contractManager.SetProvideModesWithReturnTraffic(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
		protocol.ProvideMode_Public:  true,
	})

	storedContract := &protocol.StoredContract{
		ContractId:        NewId().Bytes(),
		TransferByteCount: uint64(1024),
	}
	storedContractBytes, err := ProtoMarshal(storedContract)
	AssertEqual(t, err, nil)

	sign := func(provideMode protocol.ProvideMode) []byte {
		provideSecretKey, ok := contractManager.GetProvideSecretKey(provideMode)
		AssertEqual(t, ok, true)
		return SignStoredContract(contractManager.settings, provideSecretKey, storedContractBytes)
	}
	networkHmac := sign(protocol.ProvideMode_Network)
	publicHmac := sign(protocol.ProvideMode_Public)
	streamHmac := sign(protocol.ProvideMode_Stream)

	// not paused: all enabled modes verify
	AssertEqual(t, contractManager.Verify(networkHmac, storedContractBytes, protocol.ProvideMode_Network), true)
	AssertEqual(t, contractManager.Verify(publicHmac, storedContractBytes, protocol.ProvideMode_Public), true)
	AssertEqual(t, contractManager.Verify(streamHmac, storedContractBytes, protocol.ProvideMode_Stream), true)

	contractManager.SetProvidePaused(true)

	// paused stops public but keeps network and stream
	AssertEqual(t, contractManager.Verify(networkHmac, storedContractBytes, protocol.ProvideMode_Network), true)
	AssertEqual(t, contractManager.Verify(publicHmac, storedContractBytes, protocol.ProvideMode_Public), false)
	AssertEqual(t, contractManager.Verify(streamHmac, storedContractBytes, protocol.ProvideMode_Stream), true)

	// the paused provide frame announces only the network and stream keys
	provideFrame, err := contractManager.provideFrame()
	AssertEqual(t, err, nil)
	message, err := FromFrame(provideFrame)
	AssertEqual(t, err, nil)
	provide := message.(*protocol.Provide)
	announcedModes := []protocol.ProvideMode{}
	for _, provideKey := range provide.Keys {
		announcedModes = append(announcedModes, provideKey.Mode)
	}
	slices.Sort(announcedModes)
	AssertEqual(t, announcedModes, []protocol.ProvideMode{protocol.ProvideMode_Network, protocol.ProvideMode_Stream})

	contractManager.SetProvidePaused(false)

	// unpaused: public verifies again
	AssertEqual(t, contractManager.Verify(publicHmac, storedContractBytes, protocol.ProvideMode_Public), true)
}

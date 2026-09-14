package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func newNetworkPeerAdmissionTestChannel(
	t *testing.T,
	networkPeerDestination bool,
) (*multiClientChannel, *Client, Id) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	peerId := NewId()
	var client *Client
	generator := &TestMultiClientGenerator{
		nextDestinations: func(int, []MultiHopId, string) (map[MultiHopId]DestinationStats, error) {
			return nil, nil
		},
		newClientArgs: func() (*MultiClientGeneratorClientArgs, error) {
			return &MultiClientGeneratorClientArgs{ClientId: NewId()}, nil
		},
		removeClientArgs: func(*MultiClientGeneratorClientArgs) {},
		removeClientWithArgs: func(*Client, *MultiClientGeneratorClientArgs) {
		},
		newClientSettings: func() *ClientSettings {
			settings := DefaultClientSettings()
			settings.WebRtcSettings.NetworkPeerReceiveBufferSize = 512 * 1024
			settings.WebRtcSettings.NetworkPeerMemoryBudget =
				NewTransferMemoryBudget(2 * settings.WebRtcSettings.NetworkPeerReceiveBufferSize)
			return settings
		},
		newClient: func(
			ctx context.Context,
			args *MultiClientGeneratorClientArgs,
			settings *ClientSettings,
		) (*Client, error) {
			client = NewClient(ctx, args.ClientId, NewNoContractClientOob(), settings)
			return client, nil
		},
	}
	multiSettings := DefaultMultiClientSettings()
	multiSettings.CPingRestTimeout = time.Hour
	multiSettings.BlackholeTimeout = time.Hour
	channel, err := newMultiClientChannel(
		ctx,
		&multiClientChannelArgs{
			MultiClientGeneratorClientArgs: MultiClientGeneratorClientArgs{ClientId: NewId()},
			Destination:                    RequireMultiHopId(peerId),
			NetworkPeerDestination:         networkPeerDestination,
		},
		generator,
		func(*multiClientChannel, TransferPath, protocol.ProvideMode, TransportType, *IpPath, []byte) {},
		nil,
		DefaultSecurityPolicy(ctx),
		func(*ContractStatus) {},
		func([]*ContractStatsEvent) {},
		func() {},
		nil,
		multiSettings,
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		channel.Close()
		cancel()
	})
	return channel, client, peerId
}

func TestMultiClientSelectedNetworkPeerIsPrioritizedBeforeFirstPing(t *testing.T) {
	_, client, peerId := newNetworkPeerAdmissionTestChannel(t, true)

	client.webRtcManager.stateLock.Lock()
	_, remembered := client.webRtcManager.networkPeers[peerId]
	networkAdmission := client.webRtcManager.usesNetworkPeerAdmissionLocked(peerId)
	_, prioritized := client.webRtcManager.prioritizedPeers[peerId]
	client.webRtcManager.stateLock.Unlock()
	if !remembered || !networkAdmission || !prioritized {
		t.Fatalf(
			"selected peer remembered/network/prioritized = %v/%v/%v, want true/true/true",
			remembered,
			networkAdmission,
			prioritized,
		)
	}
}

func TestMultiClientPublicDestinationIsNotPretrustedAsNetworkPeer(t *testing.T) {
	_, client, peerId := newNetworkPeerAdmissionTestChannel(t, false)

	client.webRtcManager.stateLock.Lock()
	_, remembered := client.webRtcManager.networkPeers[peerId]
	_, prioritized := client.webRtcManager.prioritizedPeers[peerId]
	client.webRtcManager.stateLock.Unlock()
	if remembered || prioritized {
		t.Fatalf(
			"public peer remembered/prioritized = %v/%v, want false/false",
			remembered,
			prioritized,
		)
	}
}

func TestMultiClientSelectedNetworkPeerBootstrapsContractClassification(t *testing.T) {
	_, client, _ := newNetworkPeerAdmissionTestChannel(t, true)

	if !client.settings.DefaultTransferOpts.NetworkPeer {
		t.Fatal("selected same-network destination did not mark its send path as a Network peer")
	}
}

func TestMultiClientPublicDestinationDoesNotBootstrapContractClassification(t *testing.T) {
	_, client, _ := newNetworkPeerAdmissionTestChannel(t, false)

	if client.settings.DefaultTransferOpts.NetworkPeer {
		t.Fatal("public destination was incorrectly marked as a Network peer")
	}
}

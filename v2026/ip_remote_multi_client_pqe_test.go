package connect

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// testingRecordingGenerator records the client settings a window client is
// created with, and fails the creation so no real client spins up.
type testingRecordingGenerator struct {
	testingEmptyMultiClientGenerator
	clientSettings *ClientSettings
}

func (self *testingRecordingGenerator) NewClient(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
	self.clientSettings = clientSettings
	return nil, fmt.Errorf("no clients")
}

// TestMultiClientChannelPqe verifies the profile's post-quantum encryption
// setting enables the e2e encryption sessions on the window clients, and
// stays off otherwise.
func TestMultiClientChannelPqe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newChannelClientSettings := func(performanceProfile *PerformanceProfile) *ClientSettings {
		generator := &testingRecordingGenerator{}
		_, err := newMultiClientChannel(
			ctx,
			&multiClientChannelArgs{},
			generator,
			nil,
			nil,
			nil,
			nil,
			nil,
			func() {},
			performanceProfile,
			DefaultMultiClientSettings(),
			// nil falls back to the static settings; this test does not
			// exercise the runtime override, the uplink gate, the metrics,
			// the flow count, the resize wake, the receiving-sibling count,
			// or the qualification hooks
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
		// the recording generator fails creation after settings are applied
		AssertEqual(t, true, err != nil)
		AssertEqual(t, true, generator.clientSettings != nil)
		return generator.clientSettings
	}

	// no profile: encryption stays off
	clientSettings := newChannelClientSettings(nil)
	AssertEqual(t, EncryptionModeOff, clientSettings.EncryptionSettings.Mode)

	// profile without pqe: encryption stays off
	clientSettings = newChannelClientSettings(&PerformanceProfile{
		WindowType:  WindowTypeAuto,
		AllowDirect: true,
	})
	AssertEqual(t, EncryptionModeOff, clientSettings.EncryptionSettings.Mode)

	// pqe on an auto profile enables the e2e sessions
	clientSettings = newChannelClientSettings(&PerformanceProfile{
		WindowType:            WindowTypeAuto,
		PostQuantumEncryption: true,
	})
	AssertEqual(t, EncryptionModeRequired, clientSettings.EncryptionSettings.Mode)

	// pqe on a fixed profile enables the e2e sessions
	clientSettings = newChannelClientSettings(&PerformanceProfile{
		WindowType:            WindowTypeSpeed,
		WindowSize:            DefaultWindowSizeSettings(),
		PostQuantumEncryption: true,
	})
	AssertEqual(t, EncryptionModeRequired, clientSettings.EncryptionSettings.Mode)
}

// TestRejectCandidateMissingEncryptionKey pins the EncryptionCapabilityPrefilter
// decision: only the definitive "no key published" answer under
// EncryptionModeRequired rejects a window candidate — fetch errors and
// non-Required modes never do (the prefilter accelerates certain failure, it
// never admits).
func TestRejectCandidateMissingEncryptionKey(t *testing.T) {
	AssertEqual(t, true, rejectCandidateMissingEncryptionKey(EncryptionModeRequired, nil, nil))
	AssertEqual(t, true, rejectCandidateMissingEncryptionKey(EncryptionModeRequired, []byte{}, nil))
	AssertEqual(t, false, rejectCandidateMissingEncryptionKey(EncryptionModeRequired, []byte{1, 2, 3}, nil))
	AssertEqual(t, false, rejectCandidateMissingEncryptionKey(EncryptionModeRequired, nil, errors.New("api unreachable")))
	AssertEqual(t, false, rejectCandidateMissingEncryptionKey(EncryptionModeOpportunistic, nil, nil))
	AssertEqual(t, false, rejectCandidateMissingEncryptionKey(EncryptionModeOff, nil, nil))
}

// TestEncryptionCapabilityFetcher pins the channel-side prefilter plumbing: a
// bare fixture (no client) opts out; a Required client with a configured
// out-of-band key factory mints a fetcher for exactly the channel's
// destination tail, and an empty fetch result combines with the decision
// function to reject the candidate.
func TestEncryptionCapabilityFetcher(t *testing.T) {
	// bare fixture: no client, no destination
	bare := &multiClientChannel{}
	fetch, mode := bare.EncryptionCapabilityFetcher()
	AssertEqual(t, true, fetch == nil)
	AssertEqual(t, EncryptionModeOff, mode)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeRequired
	var mintedPeerId Id
	settings.EncryptionSettings.NewPeerClientPublicKeyFetcher = func(peerId Id) func(context.Context) ([]byte, error) {
		mintedPeerId = peerId
		return func(context.Context) ([]byte, error) {
			// the platform's definitive "this peer has published no key"
			return nil, nil
		}
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	destinationId := NewId()
	channel := &multiClientChannel{
		client: client,
		args: &multiClientChannelArgs{
			Destination: RequireMultiHopId(destinationId),
		},
	}
	fetch, mode = channel.EncryptionCapabilityFetcher()
	AssertEqual(t, EncryptionModeRequired, mode)
	if fetch == nil {
		t.Fatal("expected a fetcher for a Required client with a configured factory")
	}
	AssertEqual(t, destinationId, mintedPeerId)

	publicKey, fetchErr := fetch(ctx)
	AssertEqual(t, true, rejectCandidateMissingEncryptionKey(mode, publicKey, fetchErr))

	// no factory configured: no fetcher, mode still reported
	noFactorySettings := DefaultClientSettings()
	noFactorySettings.EncryptionSettings.Mode = EncryptionModeRequired
	noFactoryClient := NewClient(ctx, NewId(), NewNoContractClientOob(), noFactorySettings)
	defer noFactoryClient.Cancel()
	noFactoryChannel := &multiClientChannel{
		client: noFactoryClient,
		args: &multiClientChannelArgs{
			Destination: RequireMultiHopId(destinationId),
		},
	}
	fetch, mode = noFactoryChannel.EncryptionCapabilityFetcher()
	AssertEqual(t, true, fetch == nil)
	AssertEqual(t, EncryptionModeRequired, mode)
}

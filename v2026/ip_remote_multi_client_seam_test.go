package connect

// This file pins the ApiMultiClientGenerator measurement seams at their
// production owner. The fixtures cancel setup after transport construction,
// so no platform or api service is required.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// A generated transport settings value may be shared across windows. The
// generator must copy it before adding the client logger or P2P-only factory,
// while still applying the requested transport mode and observer.
func TestApiMultiClientGeneratorTransportSeamsCopySettings(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	generatedSettings := DefaultPlatformTransportSettings()
	generatedSettings.Log = nil
	generatedSettings.TransportGenerator = nil
	generatorSettings := DefaultApiMultiClientGeneratorSettings()
	var settingsCallCount atomic.Int32
	generatorSettings.PlatformTransportSettingsGenerator = func() *PlatformTransportSettings {
		settingsCallCount.Add(1)
		return generatedSettings
	}
	generatorSettings.PlatformTransportMode = TransportModeH1
	createdTransports := make(chan *PlatformTransport, 1)
	generatorSettings.PlatformTransportCreated = func(client *Client, transport *PlatformTransport) {
		createdTransports <- transport
	}
	strategy := NewClientStrategyWithDefaults(ctx)
	generator := NewApiMultiClientGenerator(
		ctx,
		nil,
		strategy,
		nil,
		"http://127.0.0.1:1",
		"test-jwt",
		"http://127.0.0.1:1",
		"test-device",
		"test-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		generatorSettings,
	)
	callCtx, callCancel := context.WithCancel(ctx)
	callCancel()
	clientSettings := DefaultClientSettings()
	clientSettings.ControlPingTimeout = time.Second
	_, err := generator.NewClientContext(
		ctx,
		callCtx,
		&MultiClientGeneratorClientArgs{
			ClientId: NewId(),
			ClientAuth: &ClientAuth{
				ByJwt:      "test-jwt",
				InstanceId: NewId(),
				AppVersion: "0.0.0-test",
			},
			P2pOnly: true,
		},
		clientSettings,
	)
	if err == nil {
		t.Fatal("canceled setup unexpectedly created a client")
	}
	if got := settingsCallCount.Load(); got != 1 {
		t.Fatalf("settings generator called %d times, expected one", got)
	}

	var createdTransport *PlatformTransport
	select {
	case createdTransport = <-createdTransports:
	default:
		t.Fatal("transport observer was not called")
	}
	if createdTransport.targetMode != TransportModeH1 {
		t.Fatalf("target mode = %q, expected h1", createdTransport.targetMode)
	}
	if createdTransport.settings == generatedSettings {
		t.Fatal("window transport retained the caller-owned settings pointer")
	}
	if createdTransport.settings.Log == nil {
		t.Fatal("window transport did not receive the client logger")
	}
	if createdTransport.settings.TransportGenerator == nil {
		t.Fatal("P2P-only window did not receive its control-only transport factory")
	}
	if generatedSettings.Log != nil {
		t.Fatal("window logger mutated the generated settings")
	}
	if generatedSettings.TransportGenerator != nil {
		t.Fatal("P2P-only window mutated the generated settings")
	}
	select {
	case <-createdTransport.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("failed client setup did not close the observed transport")
	}
}

// A nil generator result is treated like an absent generator, and the zero
// mode retains automatic carrier selection.
func TestApiMultiClientGeneratorTransportSeamsNilDefaults(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	generatorSettings := DefaultApiMultiClientGeneratorSettings()
	generatorSettings.PlatformTransportSettingsGenerator = func() *PlatformTransportSettings {
		return nil
	}
	createdTransports := make(chan *PlatformTransport, 1)
	generatorSettings.PlatformTransportCreated = func(client *Client, transport *PlatformTransport) {
		createdTransports <- transport
	}
	generator := NewApiMultiClientGenerator(
		ctx,
		nil,
		NewClientStrategyWithDefaults(ctx),
		nil,
		"http://127.0.0.1:1",
		"test-jwt",
		"http://127.0.0.1:1",
		"test-device",
		"test-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		generatorSettings,
	)
	callCtx, callCancel := context.WithCancel(ctx)
	callCancel()
	_, err := generator.NewClientContext(
		ctx,
		callCtx,
		&MultiClientGeneratorClientArgs{
			ClientId: NewId(),
			ClientAuth: &ClientAuth{
				ByJwt:      "test-jwt",
				InstanceId: NewId(),
				AppVersion: "0.0.0-test",
			},
		},
		DefaultClientSettings(),
	)
	if err == nil {
		t.Fatal("canceled setup unexpectedly created a client")
	}

	var createdTransport *PlatformTransport
	select {
	case createdTransport = <-createdTransports:
	default:
		t.Fatal("transport observer was not called")
	}
	if createdTransport.targetMode != TransportModeAuto {
		t.Fatalf("target mode = %q, expected automatic selection", createdTransport.targetMode)
	}
	if createdTransport.settings == nil {
		t.Fatal("nil generator result produced nil transport settings")
	}
	if createdTransport.settings.H3Port != DefaultPlatformTransportSettings().H3Port {
		t.Fatalf("H3 port = %d, expected production default", createdTransport.settings.H3Port)
	}
}

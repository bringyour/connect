package connect

import (
	"context"
	"testing"
	"time"
)

func TestPlatformH3GatePreemptsLowerPriorityFamily(t *testing.T) {
	transport := testingPlatformTransportModes()
	transport.settings = DefaultPlatformTransportSettings()
	transport.modePreferences = normalizeTransportModePreferences(nil)
	gate := newPlatformH3Gate(transport)

	dnsCtx, releaseDNS, ok := gate.Acquire(t.Context(), TransportModeH3Dns)
	if !ok {
		t.Fatal("DNS family did not acquire an empty gate")
	}
	directAcquired := make(chan struct{}, 1)
	var releaseDirect func()
	go func() {
		_, release, acquired := gate.Acquire(t.Context(), TransportModeH3)
		if acquired {
			releaseDirect = release
			close(directAcquired)
		}
	}()
	select {
	case <-dnsCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("higher-priority direct H3 did not cancel the DNS carrier")
	}
	select {
	case <-directAcquired:
		t.Fatal("direct H3 acquired before the canceled DNS owner released the socket")
	default:
	}
	releaseDNS()
	select {
	case <-directAcquired:
	case <-time.After(time.Second):
		t.Fatal("direct H3 did not acquire after DNS released the gate")
	}
	releaseDirect()
}

func TestPlatformH3GateAllowsOnlyOneFamily(t *testing.T) {
	transport := testingPlatformTransportModes()
	transport.settings = DefaultPlatformTransportSettings()
	transport.modePreferences = normalizeTransportModePreferences(map[TransportMode]int{
		TransportModeH3Dns:     2,
		TransportModeH3DnsPump: 2,
	})
	gate := newPlatformH3Gate(transport)

	_, releaseDNS, ok := gate.Acquire(t.Context(), TransportModeH3Dns)
	if !ok {
		t.Fatal("first H3 family did not acquire")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pumpAcquired := make(chan bool, 1)
	go func() {
		_, release, acquired := gate.Acquire(ctx, TransportModeH3DnsPump)
		if acquired {
			release()
		}
		pumpAcquired <- acquired
	}()
	select {
	case <-pumpAcquired:
		t.Fatal("equal-priority H3 family overlapped the active carrier")
	case <-time.After(25 * time.Millisecond):
	}
	releaseDNS()
	select {
	case acquired := <-pumpAcquired:
		if !acquired {
			t.Fatal("waiting H3 family did not acquire after release")
		}
	case <-time.After(time.Second):
		t.Fatal("waiting H3 family was not woken")
	}
}

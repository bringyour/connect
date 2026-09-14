package connect

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestDmcaEvictThenResumeMidStreamNotDropped pins the TTL-eviction user-visible edge:
// a legitimately allowed flow (TLS) that idles past FlowTtl and is reclaimed must not
// be dropped when it resumes. The resumed traffic is mid-stream ciphertext with no
// ClientHello and no SYN, so the only thing standing between the user and a broken
// long-idle connection is the sawFlowStart grace (the encrypted heuristic is only
// trusted when the flow was observed from its start).
func TestDmcaEvictThenResumeMidStreamNotDropped(t *testing.T) {
	settings := DefaultDmcaSecurityPolicySettings()
	settings.FlowTtl = 50 * time.Millisecond
	// nil ctx: no scan goroutine; the test drives eviction deterministically
	d := newDmcaDetector(nil, settings, newWebStandardDetector(DefaultWebStandardSettings()))

	path := dmcaPath(IpProtocolTcp, 41200, 8443, true)
	if r := d.inspect(path, tlsClientHello()); r != SecurityPolicyResultAllow {
		t.Fatalf("tls flow -> %v, want allow", r)
	}
	if n := d.flowCount(); n != 1 {
		t.Fatalf("tracked flows = %d, want 1", n)
	}

	// idle past FlowTtl; the scan reclaims the flow state (terminal verdict included)
	d.evictIdle(time.Now().Add(time.Hour))
	if n := d.flowCount(); n != 0 {
		t.Fatalf("tracked flows after eviction = %d, want 0", n)
	}

	// resume mid-stream: no SYN, no ClientHello, pure ciphertext. Every resumed packet
	// must pass (dropping any one of them breaks the live connection).
	resumed := dmcaPath(IpProtocolTcp, 41200, 8443, false)
	for i := 0; i < settings.InspectionPacketBudget+2; i += 1 {
		if r := d.inspect(resumed, encryptedPayload(512)); r != SecurityPolicyResultAllow {
			t.Fatalf("resumed mid-stream packet %d -> %v, want allow (sawFlowStart grace)", i, r)
		}
	}
}

// TestDmcaIngressOnlyActivityAcrossScans pins the download-heavy case across multiple
// scan rounds: a flow whose only activity is return-direction (ingress) traffic must
// survive every idle scan, and must be reclaimed once both directions go quiet.
func TestDmcaIngressOnlyActivityAcrossScans(t *testing.T) {
	settings := DefaultDmcaSecurityPolicySettings()
	settings.FlowTtl = 40 * time.Millisecond
	d := newDmcaDetector(nil, settings, newWebStandardDetector(DefaultWebStandardSettings()))

	eg := dmcaPath(IpProtocolTcp, 41300, 9443, true)
	d.classify(eg, tlsClientHello())

	// several TTL windows with only ingress refreshes between scans
	for round := 0; round < 3; round += 1 {
		time.Sleep(25 * time.Millisecond)
		d.touchIngress(eg.Reverse())
		d.evictIdle(time.Now())
		if n := d.flowCount(); n != 1 {
			t.Fatalf("round %d: ingress-only-active flow evicted (flows=%d)", round, n)
		}
	}

	// quiet in both directions -> reclaimed
	time.Sleep(60 * time.Millisecond)
	d.evictIdle(time.Now())
	if n := d.flowCount(); n != 0 {
		t.Fatalf("idle flow not reclaimed (flows=%d)", n)
	}
}

// TestDmcaCapacityLruBoundWithTtl pins the interplay of the two eviction mechanisms:
// the capacity-LRU keeps the table bounded during a fill burst regardless of TTL, and
// the TTL scan then drains what the LRU kept.
func TestDmcaCapacityLruBoundWithTtl(t *testing.T) {
	settings := DefaultDmcaSecurityPolicySettings()
	settings.MaxFlows = dmcaFlowShards * 4
	settings.FlowTtl = time.Hour // TTL never fires during the fill
	d := newDmcaDetector(nil, settings, newWebStandardDetector(DefaultWebStandardSettings()))

	for i := 0; i < settings.MaxFlows*4; i += 1 {
		d.classify(dmcaPath(IpProtocolTcp, 20000+i, 9000, true), encryptedPayload(64))
	}
	if n := d.flowCount(); settings.MaxFlows < n {
		t.Fatalf("capacity LRU did not bound the table: flows=%d, max=%d", n, settings.MaxFlows)
	}
	if n := d.flowCount(); n == 0 {
		t.Fatal("fill tracked no flows")
	}

	d.evictIdle(time.Now().Add(2 * time.Hour))
	if n := d.flowCount(); n != 0 {
		t.Fatalf("TTL scan did not drain the LRU-bounded table: flows=%d", n)
	}
}

// TestDmcaConcurrentClassifyRefreshEvict hammers classify, both refresh directions, and
// the idle scan concurrently. It asserts nothing beyond termination and a coherent count:
// its value is under -race, where it catches locking mistakes between the scan's map
// iteration+delete, refresh's read-then-atomic-store, and classify's create path.
func TestDmcaConcurrentClassifyRefreshEvict(t *testing.T) {
	settings := DefaultDmcaSecurityPolicySettings()
	settings.FlowTtl = 5 * time.Millisecond
	settings.MaxFlows = 256
	d := newDmcaDetector(nil, settings, newWebStandardDetector(DefaultWebStandardSettings()))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w += 1 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				// a mix of shared and distinct flows across workers
				p := dmcaPath(IpProtocolUdp, 30000+(i%64), 9000+w%2, false)
				d.classify(p, encryptedPayload(128))
				d.touchEgress(p)
				d.touchIngress(p.Reverse())
				i += 1
			}
		}(w)
	}
	for w := 0; w < 2; w += 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				d.evictIdle(time.Now())
				time.Sleep(time.Millisecond)
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	if n := d.flowCount(); n < 0 || settings.MaxFlows < n {
		t.Fatalf("flow count incoherent after concurrent load: %d", n)
	}
}

// TestSecurityPolicyFlowCountHook pins the Testing_FlowCount hook on both the client
// policy and its Reverse wrapper, which the sdk-level tests rely on for exact
// fill/reclaim assertions.
func TestSecurityPolicyFlowCountHook(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dmca := DefaultDmcaSecurityPolicySettings()
	dmca.FlowTtl = 0 // no scan goroutine
	policy := NewSecurityPolicy(ctx, DefaultCfaaSecurityPolicySettings(), dmca, DefaultWebStandardSettings(), DefaultSecurityPolicyStatsCollector())

	counter, ok := policy.(interface{ Testing_FlowCount() int })
	if !ok {
		t.Fatal("securityPolicy does not expose Testing_FlowCount")
	}
	if n := counter.Testing_FlowCount(); n != 0 {
		t.Fatalf("initial flow count = %d, want 0", n)
	}

	const flows = 10
	for i := 0; i < flows; i += 1 {
		path := dmcaPath(IpProtocolTcp, 42000+i, 8443, true)
		if _, err := policy.InspectEgress(protocol.ProvideMode_Public, path, tlsClientHello()); err != nil {
			t.Fatal(err)
		}
	}
	if n := counter.Testing_FlowCount(); n != flows {
		t.Fatalf("flow count = %d, want %d", n, flows)
	}

	revCounter, ok := Reverse(policy).(interface{ Testing_FlowCount() int })
	if !ok {
		t.Fatal("reverse policy does not forward Testing_FlowCount")
	}
	if n := revCounter.Testing_FlowCount(); n != flows {
		t.Fatalf("reverse flow count = %d, want %d", n, flows)
	}
}

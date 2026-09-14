// The resource bounds of the udp 53 forwarder (EXTENDER.md A6): the limiter
// state a flood of source addresses may keep, the budget one resolution holds
// a worker for, and the same per-source rules for an ipv6 source.

package extender

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// The limiter state of a source that is neither in flight nor short of tokens
// says nothing, so a flood of addresses cannot grow the map without bound
// (A6). A source with a query in flight is state that still says something and
// survives the prune.
func TestExtenderForwarderBoundsItsSourceStates(t *testing.T) {
	if extenderDnsMaxSourceCount != 4096 {
		t.Errorf("kept sources = %d, expected 4096", extenderDnsMaxSourceCount)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(ctx, nil, nil, nil, nil, DefaultExtenderSettings())
	defer server.Close()
	forwarder := &extenderDnsForwarder{
		server:       server,
		sourceStates: map[string]*extenderDnsSource{},
	}

	now := time.Now()
	// one source holds a query in flight for the whole flood
	heldSource := "2001:db8:1::1"
	if err := forwarder.admit(heldSource, now); err != nil {
		t.Fatalf("the held source was refused: %v", err)
	}

	floodSourceCount := extenderDnsMaxSourceCount + 64
	maxSourceCount := 0
	for i := range floodSourceCount {
		// every source is a second after the last, so a source that has been
		// released has earned its burst back by the time the prune reaches it
		now = now.Add(time.Second)
		source := fmt.Sprintf("2001:db8:2::%x", i+1)
		if err := forwarder.admit(source, now); err != nil {
			t.Fatalf("%s was refused: %v", source, err)
		}
		forwarder.release(source)
		if count := len(forwarder.sourceStates); maxSourceCount < count {
			maxSourceCount = count
		}
	}

	if extenderDnsMaxSourceCount < maxSourceCount {
		t.Fatalf(
			"%d sources were kept for %d addresses, over the bound %d",
			maxSourceCount,
			floodSourceCount,
			extenderDnsMaxSourceCount,
		)
	}
	if count := len(forwarder.sourceStates); extenderDnsMaxSourceCount < count {
		t.Fatalf("sources after the flood = %d, over the bound %d", count, extenderDnsMaxSourceCount)
	}
	heldState, ok := forwarder.sourceStates[heldSource]
	if !ok {
		t.Fatal("the prune dropped a source with a query in flight")
	}
	if !heldState.inFlight {
		t.Fatal("the held source no longer reports its query in flight")
	}
	// the in flight rule still holds for it, which is what the kept state is for
	if err := forwarder.admit(heldSource, now); err == nil {
		t.Fatal("a second query was admitted for the source that is still in flight")
	}
}

// One resolution is bounded, which is what frees the worker and the source's
// in flight slot when a resolver never answers (A6). Without it the one worker
// here would never take another query.
func TestExtenderForwarderTimeoutFreesItsWorker(t *testing.T) {
	answerBytes := newTestDnsAnswer(
		t,
		testForwardName,
		dnsmessage.TypeA,
		netip.MustParseAddr("192.0.2.40"),
		1,
	)
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var forwardLock sync.Mutex
	forwardCount := 0

	settings := DefaultExtenderSettings()
	settings.DnsForwardWorkerCount = 1
	settings.DnsForwardTimeout = 250 * time.Millisecond
	// the worker pool is what is under test here, not the rates
	settings.DnsMaxQueryRatePerSource = 0
	settings.DnsMaxQueryRate = 0
	settings.DnsForward = func(ctx context.Context, _ dnsmessage.Type, _ string) ([]byte, bool) {
		first := func() bool {
			forwardLock.Lock()
			defer forwardLock.Unlock()
			forwardCount += 1
			return forwardCount == 1
		}()
		if !first {
			return answerBytes, true
		}
		select {
		case started <- struct{}{}:
		default:
		}
		// the first resolution holds the only worker; nothing but its own
		// budget ends it
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, false
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(ctx, nil, nil, nil, nil, settings)
	defer server.CloseAndWait()
	defer close(release)
	packetConn := newRecordingPacketConn()
	forwarder := newExtenderDnsForwarder(server, packetConn)
	defer forwarder.close()

	queryBytes := newTestDnsQuery(t, 0x8881, testForwardName, dnsmessage.TypeA)
	forwarder.handleQuery(queryBytes, &net.UDPAddr{IP: net.ParseIP("192.0.2.41"), Port: 1053})
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the first query never reached the forward")
	}

	// the second query waits in the queue the one worker drains, so it can
	// only be answered once the first resolution's budget frees that worker
	laterQueryBytes := newTestDnsQuery(t, 0x8882, testForwardName, dnsmessage.TypeA)
	forwarder.handleQuery(laterQueryBytes, &net.UDPAddr{IP: net.ParseIP("192.0.2.42"), Port: 1053})
	select {
	case answer := <-packetConn.answers:
		var parser dnsmessage.Parser
		header, err := parser.Start(answer)
		if err != nil {
			t.Fatal(err)
		}
		if header.ID != 0x8882 {
			t.Fatalf("answer id = %#x, expected the query that waited", header.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the resolution budget did not free the worker")
	}
}

// The per-source rules of A6 key on the address alone, so an ipv6 source has
// its own budget and two of its ports share one.
func TestExtenderForwarderLimitsAnIpv6Source(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// the per-source bucket: one query, and the next token is a thousand
	// seconds away
	rateSettings := DefaultExtenderSettings()
	rateSettings.DnsMaxQueryBurstPerSource = 1
	rateSettings.DnsMaxQueryRatePerSource = 0.001
	rateSettings.DnsMaxQueryRate = 0
	rateServer := NewExtenderServer(ctx, nil, nil, nil, nil, rateSettings)
	defer rateServer.Close()
	rateForwarder := &extenderDnsForwarder{
		server:       rateServer,
		sourceStates: map[string]*extenderDnsSource{},
	}

	now := time.Now()
	if err := rateForwarder.admit("2001:db8::1", now); err != nil {
		t.Fatalf("the first ipv6 query was refused: %v", err)
	}
	rateForwarder.release("2001:db8::1")
	err := rateForwarder.admit("2001:db8::1", now)
	if err == nil {
		t.Fatal("an ipv6 source over its burst was admitted")
	}
	if !strings.Contains(err.Error(), "2001:db8::1 is over its query rate") {
		t.Fatalf("attributed error = %v, expected the per-source rate of the ipv6 address", err)
	}
	// another ipv6 source spends its own budget
	if err := rateForwarder.admit("2001:db8::2", now); err != nil {
		t.Fatalf("another ipv6 source was refused: %v", err)
	}

	// the source of a query is the address without its port, so one ipv6
	// address cannot hold two queries in flight by using two ports
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	extenderErrors := make(chan error, 8)
	inFlightSettings := DefaultExtenderSettings()
	inFlightSettings.DnsForwardWorkerCount = 1
	inFlightSettings.DnsForwardTimeout = 60 * time.Second
	inFlightSettings.DnsMaxQueryRatePerSource = 0
	inFlightSettings.DnsMaxQueryRate = 0
	inFlightSettings.DnsForward = func(ctx context.Context, _ dnsmessage.Type, _ string) ([]byte, bool) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, false
	}
	inFlightSettings.ErrorHandler = func(_ string, err error) {
		select {
		case extenderErrors <- err:
		default:
		}
	}
	inFlightServer := NewExtenderServer(ctx, nil, nil, nil, nil, inFlightSettings)
	defer inFlightServer.CloseAndWait()
	defer close(release)
	inFlightForwarder := newExtenderDnsForwarder(inFlightServer, newRecordingPacketConn())
	defer inFlightForwarder.close()

	queryBytes := newTestDnsQuery(t, 0x9991, testForwardName, dnsmessage.TypeA)
	inFlightForwarder.handleQuery(queryBytes, &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 1053})
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the ipv6 query never reached the forward")
	}
	inFlightForwarder.handleQuery(queryBytes, &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 2053})
	select {
	case err := <-extenderErrors:
		if !strings.Contains(err.Error(), "2001:db8::1 already has a query in flight") {
			t.Fatalf("attributed error = %v, expected the in flight rule of the ipv6 address", err)
		}
	default:
		t.Fatal("another port of the same ipv6 source was admitted alongside its query in flight")
	}
}

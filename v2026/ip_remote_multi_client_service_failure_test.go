package connect

import (
	"context"
	"net"
	"testing"
	"time"
)

func destinationServiceTestPath(version int, destination string, port int) *IpPath {
	source := "10.0.0.2"
	if version == 6 {
		source = "fd75:726e:6574:776f:726b::20"
	}
	return &IpPath{
		Version:         version,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP(source),
		SourcePort:      49152,
		DestinationIp:   net.ParseIP(destination),
		DestinationPort: port,
		Syn:             true,
	}
}

func TestDestinationServiceFailureIsScopedByAddressAndPort(t *testing.T) {
	settings := DefaultMultiClientSettings()
	failedExit := &multiClientChannel{ctx: context.Background(), settings: settings}
	healthyExit := &multiClientChannel{ctx: context.Background(), settings: settings}
	parent := &RemoteUserNatMultiClient{
		settings:                   settings,
		destinationServiceFailures: map[destinationServiceFailureKey]destinationServiceFailure{},
	}

	v4Port465 := destinationServiceTestPath(4, "203.0.113.10", 465)
	parent.stateLock.Lock()
	parent.recordDestinationServiceFailureWithLock(failedExit, v4Port465)
	parent.stateLock.Unlock()

	got := parent.filterDestinationServiceFailures(
		[]*multiClientChannel{failedExit, healthyExit},
		v4Port465,
	)
	if len(got) != 1 || got[0] != healthyExit {
		t.Fatalf("v4 tcp/465 candidates = %p, want only healthy exit %p", got, healthyExit)
	}

	// The same address on a different SMTP service is independent.
	v4Port587 := destinationServiceTestPath(4, "203.0.113.10", 587)
	got = parent.filterDestinationServiceFailures(
		[]*multiClientChannel{failedExit, healthyExit},
		v4Port587,
	)
	if len(got) != 2 {
		t.Fatalf("tcp/587 inherited tcp/465 failure: %d candidates", len(got))
	}

	// A failure for one destination must not suppress another server using the
	// same service port.
	otherAddressPort465 := destinationServiceTestPath(4, "203.0.113.11", 465)
	got = parent.filterDestinationServiceFailures(
		[]*multiClientChannel{failedExit, healthyExit},
		otherAddressPort465,
	)
	if len(got) != 2 {
		t.Fatalf("other destination inherited TCP/465 failure: %d candidates", len(got))
	}
}

func TestDestinationServiceFailureAllFailedRetriesOldest(t *testing.T) {
	settings := DefaultMultiClientSettings()
	first := &multiClientChannel{ctx: context.Background(), settings: settings}
	second := &multiClientChannel{ctx: context.Background(), settings: settings}
	path := destinationServiceTestPath(4, "203.0.113.25", 587)
	parent := &RemoteUserNatMultiClient{
		settings:                   settings,
		destinationServiceFailures: map[destinationServiceFailureKey]destinationServiceFailure{},
	}
	firstKey, _ := destinationServiceFailureKeyFor(path, first)
	secondKey, _ := destinationServiceFailureKeyFor(path, second)
	parent.destinationServiceFailures[firstKey] = destinationServiceFailure{time: time.Now().Add(-time.Minute)}
	parent.destinationServiceFailures[secondKey] = destinationServiceFailure{time: time.Now()}

	got := parent.filterDestinationServiceFailures([]*multiClientChannel{second, first}, path)
	if len(got) != 2 || got[0] != first || got[1] != second {
		t.Fatalf("all-failed order = %p, want oldest failure first", got)
	}
}

func TestDialFailureRecordsDestinationServiceForNextSmtpFlow(t *testing.T) {
	egress := destinationServiceTestPath(4, "203.0.113.40", 465)
	parent, failedExit, _, _ := dialFailureTestParent(t, true, egress)
	healthyExit := &multiClientChannel{ctx: context.Background(), settings: parent.settings}

	parent.clientDialFailure(failedExit, egress)
	got := parent.filterDestinationServiceFailures(
		[]*multiClientChannel{failedExit, healthyExit},
		egress,
	)
	if len(got) != 1 || got[0] != healthyExit {
		t.Fatalf("post-rerace candidates = %p, want only alternate exit", got)
	}
}

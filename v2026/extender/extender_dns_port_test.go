// The dns carrier ports the extender binds (EXTENDER.md L2): 4053 always --
// whatever unprivileged port the caller configured -- and 53 as well where the
// platform allows it without privilege. A failure of the privileged bind is
// reported and survived, and only the ports that bound are advertised.

package extender

import (
	"context"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026"
)

// One extender with only the dns carrier, bound through the listen seam so no
// privileged port is ever taken. `privilegedListen` answers the 53 bind.
type dnsPortFixture struct {
	server *ExtenderServer
	// the configured unprivileged port, which stands in for 4053
	dnsPort      int
	listenErrs   chan error
	listenedPort chan int
	serveDone    chan error
}

func newDnsPortFixture(
	t *testing.T,
	dnsPrivilegedPort bool,
	privilegedListen func() (net.PacketConn, error),
) *dnsPortFixture {
	t.Helper()
	dnsPacketConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &dnsPortFixture{
		dnsPort:      dnsPacketConn.LocalAddr().(*net.UDPAddr).Port,
		listenErrs:   make(chan error, 8),
		listenedPort: make(chan int, 8),
		serveDone:    make(chan error, 1),
	}

	settings := DefaultExtenderSettings()
	settings.DnsTlds = []string{testDnsTld}
	settings.DnsPrivilegedPort = dnsPrivilegedPort
	settings.ListenPacket = func(network string, address string) (net.PacketConn, error) {
		switch address {
		case fmt.Sprintf(":%d", fixture.dnsPort):
			fixture.listenedPort <- fixture.dnsPort
			return dnsPacketConn, nil
		case fmt.Sprintf(":%d", connect.DefaultDnsPort):
			fixture.listenedPort <- connect.DefaultDnsPort
			if privilegedListen == nil {
				return nil, fmt.Errorf("the privileged port was not expected")
			}
			return privilegedListen()
		default:
			return nil, fmt.Errorf("unexpected extender listen packet %s %s", network, address)
		}
	}
	settings.ListenErrorHandler = func(carrier string, err error) {
		fixture.listenErrs <- fmt.Errorf("%s: %w", carrier, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	fixture.server = NewExtenderServer(
		ctx,
		nil,
		[]string{"dest.example"},
		map[int][]connect.ExtenderConnectMode{
			fixture.dnsPort: {connect.ExtenderConnectModeDns},
		},
		&net.Dialer{},
		settings,
	)
	go func() {
		fixture.serveDone <- fixture.server.ListenAndServe()
	}()
	t.Cleanup(func() {
		fixture.server.CloseAndWait()
		cancel()
		select {
		case err := <-fixture.serveDone:
			if err != nil {
				t.Errorf("extender server: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("extender server did not stop")
		}
	})

	select {
	case <-fixture.server.Listening():
	case <-time.After(15 * time.Second):
		t.Fatal("the extender carriers did not bind")
	}
	return fixture
}

// The ports the fixture's listen seam was asked for, once binding has settled.
func (self *dnsPortFixture) listenedPorts() []int {
	ports := []int{}
	for {
		select {
		case port := <-self.listenedPort:
			ports = append(ports, port)
		default:
			slices.Sort(ports)
			return ports
		}
	}
}

// By default only the configured unprivileged port is bound, which is 4053 in
// production and an ephemeral port here.
func TestExtenderBindsTheUnprivilegedDnsPortAlone(t *testing.T) {
	fixture := newDnsPortFixture(t, false, nil)

	if ports := fixture.listenedPorts(); !slices.Equal(ports, []int{fixture.dnsPort}) {
		t.Fatalf("bound ports = %v, expected the configured port alone", ports)
	}
	if dnsPorts := fixture.server.DnsPorts(); !slices.Equal(dnsPorts, []int{fixture.dnsPort}) {
		t.Fatalf("dns ports = %v, expected the configured port alone", dnsPorts)
	}
	if carriers := fixture.server.Carriers(); !slices.Equal(carriers, []string{connect.ExtenderCarrierDns}) {
		t.Fatalf("carriers = %v", carriers)
	}
}

// Where the platform allows it, 53 is bound beside the unprivileged port and
// both are advertised, 53 first.
func TestExtenderBindsThePrivilegedDnsPort(t *testing.T) {
	privilegedPacketConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { privilegedPacketConn.Close() })
	fixture := newDnsPortFixture(t, true, func() (net.PacketConn, error) {
		return privilegedPacketConn, nil
	})

	expected := []int{connect.DefaultDnsPort, fixture.dnsPort}
	slices.Sort(expected)
	if ports := fixture.listenedPorts(); !slices.Equal(ports, expected) {
		t.Fatalf("bound ports = %v, expected %v", ports, expected)
	}
	if dnsPorts := fixture.server.DnsPorts(); !slices.Equal(dnsPorts, expected) {
		t.Fatalf("dns ports = %v, expected %v", dnsPorts, expected)
	}
	if dnsPorts := fixture.server.DnsPorts(); dnsPorts[0] != connect.DefaultDnsPort {
		t.Fatalf("dns ports = %v, expected 53 first", dnsPorts)
	}
	if carriers := fixture.server.Carriers(); !slices.Equal(carriers, []string{connect.ExtenderCarrierDns}) {
		t.Fatalf("carriers = %v", carriers)
	}
	select {
	case err := <-fixture.listenErrs:
		t.Fatalf("a successful bind reported %v", err)
	default:
	}
}

// A 53 bind that is refused is reported and survived: the carrier keeps
// serving on its unprivileged port and advertises only that one.
func TestExtenderPrivilegedDnsPortBindFailureIsNotFatal(t *testing.T) {
	fixture := newDnsPortFixture(t, true, func() (net.PacketConn, error) {
		return nil, fmt.Errorf("permission denied")
	})

	select {
	case err := <-fixture.listenErrs:
		if err == nil {
			t.Fatal("the bind failure was not reported")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the privileged bind failure was never reported")
	}
	if dnsPorts := fixture.server.DnsPorts(); !slices.Equal(dnsPorts, []int{fixture.dnsPort}) {
		t.Fatalf("dns ports = %v, expected the port that bound", dnsPorts)
	}
	if carriers := fixture.server.Carriers(); !slices.Equal(carriers, []string{connect.ExtenderCarrierDns}) {
		t.Fatalf("carriers = %v, expected the carrier to keep serving", carriers)
	}
	// the carrier is serving, so it has no standing bind failure to render
	if listenErrs := fixture.server.ListenErrors(); 0 < len(listenErrs) {
		t.Fatalf("listen errors = %v, expected none for a serving carrier", listenErrs)
	}
	select {
	case err := <-fixture.serveDone:
		t.Fatalf("the server exited with %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

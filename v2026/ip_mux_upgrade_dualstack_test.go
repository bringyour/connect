package connect

// ip_mux_upgrade_dualstack_test.go — family-parameterized scaffolding for the
// mux and multi-client test conversions (IPV6.md D3): loopback httptest servers
// on either family, family-addressed dns query packets, crafted udp packets,
// and the family forms of the test literals. Everything here is a pure
// function of the ip version so a converted test reads the same under v4 and
// v6 subtests.

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"testing"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"
	"github.com/gorilla/websocket"
	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/net/icmp"
)

// testFamilyIp maps a v4 test literal to the family under test: the literal
// itself for v4, and the same 32 bits under the 2001:db8::/96 documentation
// prefix for v6, canonicalized so it compares equal to what a resolver hands
// back.
func testFamilyIp(ipVersion int, ipv4 string) string {
	if ipVersion == 4 {
		return ipv4
	}
	return netip.MustParseAddr("2001:db8::" + ipv4).String()
}

// testClientIp is the tunnel-side client address crafted dns and http packets
// carry: the v4 link-local the existing tests use, or an address in the sdk's
// tunnel ULA for v6.
func testClientIp(ipVersion int) net.IP {
	if ipVersion == 4 {
		return net.ParseIP("169.254.9.9")
	}
	return net.ParseIP("fd00:7572:6e65::9:9")
}

// testDnsServerIp is the resolver address a crafted dns query is addressed to.
// The mux claims udp/53 in both families whatever the destination, so any
// routable literal works; the v6 one is the sdk's in-tunnel resolver identity.
func testDnsServerIp(ipVersion int) net.IP {
	if ipVersion == 4 {
		return net.ParseIP("10.0.0.1")
	}
	return net.ParseIP("2001:db8::65:49:70:65")
}

// testDnsDialAddress is testDnsServerIp as a host:port dial target.
func testDnsDialAddress(ipVersion int) string {
	return net.JoinHostPort(testDnsServerIp(ipVersion).String(), "53")
}

// testDnsQType is the address record type of the family as a wire question
// type. testDnsRecordType (net_dualstack_httptest_test.go) is the same thing
// as the DohCache query string; the converted dns tests ask for the family's
// own records so the AAAA pipeline is exercised under v6, not only the packet
// family.
func testDnsQType(ipVersion int) dnsmessage.Type {
	if ipVersion == 4 {
		return dnsmessage.TypeA
	}
	return dnsmessage.TypeAAAA
}

// testExampleComIp is example.com's published address of the family, used
// where a test wants a real-looking public destination.
func testExampleComIp(ipVersion int) string {
	if ipVersion == 4 {
		return "93.184.216.34"
	}
	return "2606:2800:220:1:248:1893:25c8:1946"
}

// dnsQueryPacketFromVersion crafts the udp dns query packet for name with the
// record type, transaction id and client source port controlled, addressed
// over the family (see dnsQueryPacketFrom for the v4-only A form).
func dnsQueryPacketFromVersion(t *testing.T, ipVersion int, name string, qtype dnsmessage.Type, id uint16, sourcePort int) []byte {
	t.Helper()
	qb := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: id, RecursionDesired: true})
	if err := qb.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := qb.Question(dnsmessage.Question{
		Name:  dnsmessage.MustNewName(name),
		Type:  qtype,
		Class: dnsmessage.ClassINET,
	}); err != nil {
		t.Fatal(err)
	}
	queryPayload, err := qb.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return ipOosPacket(&IpPath{
		Version:         ipVersion,
		Protocol:        IpProtocolUdp,
		SourceIp:        testClientIp(ipVersion),
		SourcePort:      sourcePort,
		DestinationIp:   testDnsServerIp(ipVersion),
		DestinationPort: 53,
	}, queryPayload)
}

// dnsQueryPacketVersion is dnsQueryPacket over the family: the address query
// for the family's own record type, from the family's client literal.
func dnsQueryPacketVersion(t *testing.T, ipVersion int, name string) []byte {
	t.Helper()
	return dnsQueryPacketFromVersion(t, ipVersion, name, testDnsQType(ipVersion), 0x1234, 33333)
}

// parseDnsReplyTyped extracts (client port, transaction id, answer records of
// the type) from a downstream dns reply packet.
func parseDnsReplyTyped(t *testing.T, packet []byte, qtype dnsmessage.Type) (clientPort int, id uint16, addrs []netip.Addr) {
	t.Helper()
	ipPath, payload, err := ParseIpPathWithPayload(packet)
	if err != nil {
		t.Fatalf("parse reply packet: %v", err)
	}
	var p dnsmessage.Parser
	header, err := p.Start(payload)
	if err != nil {
		t.Fatalf("parse reply dns: %v", err)
	}
	result := parseDohWire(payload, qtype)
	for addr := range result.AddrTtls {
		addrs = append(addrs, addr)
	}
	return ipPath.DestinationPort, header.ID, addrs
}

// localDohTlsResolverSettings trusts and uses a local tls doh server, filing
// its url under the list of the family it listens on. This is the
// certificate-trusting form; localDohResolverSettings
// (net_dualstack_httptest_test.go) takes plain urls.
func localDohTlsResolverSettings(ipVersion int, server *httptest.Server) *DnsResolverSettings {
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	dns := &DnsResolverSettings{
		EnableLocalDoh: true,
		TlsConfig:      &tls.Config{RootCAs: pool},
	}
	if ipVersion == 6 {
		dns.LocalDohUrlsIpv6 = []string{server.URL}
	} else {
		dns.LocalDohUrlsIpv4 = []string{server.URL}
	}
	return dns
}

// newIpMuxPacketVersion is a bare ip header of the family (no transport) for
// the mux's return path, the family form of newIpMuxIpv4Packet.
func newIpMuxPacketVersion(ipVersion int, sourceIp net.IP, destinationIp net.IP) []byte {
	if ipVersion == 4 {
		return newIpMuxIpv4Packet(sourceIp, destinationIp)
	}
	packet := make([]byte, Ipv6HeaderSize)
	writeIpv6Header(packet, ipProtocolNumberUdp, sourceIp.To16(), destinationIp.To16())
	return packet
}

// testingUdpPacket is testingUdp4Packet over the family: a gopacket-built udp
// packet from sourceIp to destinationIp:destinationPort.
func testingUdpPacket(ipVersion int, sourceIp string, destinationIp string, destinationPort int, payload []byte) []byte {
	if ipVersion == 4 {
		return testingUdp4Packet(sourceIp, destinationIp, destinationPort, payload)
	}
	ip := &layers.IPv6{
		Version:    6,
		HopLimit:   64,
		SrcIP:      net.ParseIP(sourceIp).To16(),
		DstIP:      net.ParseIP(destinationIp).To16(),
		NextHeader: layers.IPProtocolUDP,
	}
	udp := &layers.UDP{
		SrcPort: layers.UDPPort(40000),
		DstPort: layers.UDPPort(destinationPort),
	}
	udp.SetNetworkLayerForChecksum(ip)
	buffer := gopacket.NewSerializeBuffer()
	err := gopacket.SerializeLayers(
		buffer,
		gopacket.SerializeOptions{ComputeChecksums: true, FixLengths: true},
		ip,
		udp,
		gopacket.Payload(payload),
	)
	if err != nil {
		panic(err)
	}
	packet := make([]byte, len(buffer.Bytes()))
	copy(packet, buffer.Bytes())
	return packet
}

// requireIcmpEgressVersion is requireIcmpEgress for the family: the icmp
// buffers open an unprivileged icmp socket of that family.
func requireIcmpEgressVersion(t *testing.T, ipVersion int) {
	t.Helper()
	network := "udp4"
	if ipVersion == 6 {
		network = "udp6"
	}
	if probe, err := icmp.ListenPacket(network, ""); err != nil {
		t.Skipf("no unprivileged %s icmp socket: %v", network, err)
	} else {
		probe.Close()
	}
}

// newTestingPlatformServerOnFamily is newTestingPlatformServer bound to the
// loopback of the family: the same silent websocket platform stand-in, so a
// transport test can dial the platform over v6.
func newTestingPlatformServerOnFamily(t *testing.T, ipVersion int) *testingPlatformServer {
	t.Helper()
	platform := &testingPlatformServer{}
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	platform.server = newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if platform.rejecting.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		platform.connectCount.Add(1)
		func() {
			platform.stateLock.Lock()
			defer platform.stateLock.Unlock()
			platform.conns = append(platform.conns, ws)
		}()
		for {
			_, message, err := ws.ReadMessage()
			if err != nil {
				ws.Close()
				return
			}
			if len(message) == 0 {
				platform.emptyMessages.Add(1)
			} else {
				platform.dataMessages.Add(1)
			}
		}
	}))
	platform.url = "ws" + strings.TrimPrefix(platform.server.URL, "http")
	t.Cleanup(func() {
		platform.closeConns()
		platform.server.Close()
	})
	return platform
}

// testLoopbackPortAddress joins the family's loopback with a port, for tests
// that hold a port as an int.
func testLoopbackPortAddress(ipVersion int, port int) string {
	return net.JoinHostPort(testLoopbackIp(ipVersion), strconv.Itoa(port))
}

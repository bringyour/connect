package connect

import (
	"context"
	"net"
	"net/netip"
	"slices"
	"testing"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"

	"github.com/urnetwork/connect/v2026/protocol"
)

// buildClientHello hand-crafts a minimal but well-formed TLS ClientHello record
// carrying a single host_name SNI, so the SNI parser and sniffer can be exercised
// without a real TLS stack.
func buildClientHello(sni string) []byte {
	return buildClientHelloWithExtraExtensions(sni, nil)
}

// buildClientHelloWithEch is buildClientHello plus an encrypted_client_hello
// extension AFTER server_name (the harder ordering: a first-win server_name
// scan would miss it), so `sni` plays the OUTER (client-facing) name.
func buildClientHelloWithEch(outerSni string) []byte {
	echPayload := []byte{0x01, 0x02, 0x03, 0x04} // opaque; only the type matters
	echExt := []byte{
		byte(tlsExtensionEncryptedClientHello >> 8), byte(tlsExtensionEncryptedClientHello & 0xff),
		byte(len(echPayload) >> 8), byte(len(echPayload)),
	}
	echExt = append(echExt, echPayload...)
	return buildClientHelloWithExtraExtensions(outerSni, echExt)
}

// buildClientHelloWithExtraExtensions builds the hello with `extraExtensions`
// (raw wire extension bytes) appended after the server_name extension.
func buildClientHelloWithExtraExtensions(sni string, extraExtensions []byte) []byte {
	// server_name extension data: ServerNameList { host_name entry }
	entry := []byte{tlsSniTypeHostName, byte(len(sni) >> 8), byte(len(sni))}
	entry = append(entry, []byte(sni)...)
	snExt := []byte{byte(len(entry) >> 8), byte(len(entry))}
	snExt = append(snExt, entry...)
	// extension: type(2) + length(2) + data
	ext := []byte{0x00, 0x00, byte(len(snExt) >> 8), byte(len(snExt))}
	ext = append(ext, snExt...)
	ext = append(ext, extraExtensions...)
	// extensions block: 2-byte length + extensions
	extensions := []byte{byte(len(ext) >> 8), byte(len(ext))}
	extensions = append(extensions, ext...)
	// handshake body
	body := []byte{0x03, 0x03}                  // client_version TLS 1.2
	body = append(body, make([]byte, 32)...)    // random
	body = append(body, 0x00)                   // session_id length 0
	body = append(body, 0x00, 0x02, 0x00, 0x2f) // cipher_suites: len 2, one suite
	body = append(body, 0x01, 0x00)             // compression_methods: len 1, null
	body = append(body, extensions...)
	// handshake header: ClientHello(1) + 3-byte length
	hs := []byte{tlsHandshakeClientHello, byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))}
	hs = append(hs, body...)
	// record header: handshake(0x16) + version(0x0301) + 2-byte length
	rec := []byte{tlsRecordTypeHandshake, tlsVersionMajor, 0x01, byte(len(hs) >> 8), byte(len(hs))}
	rec = append(rec, hs...)
	return rec
}

// tls443Packet wraps a TCP payload as an IPv4 TCP segment destined for :443, as the mux
// sees it on the send path.
func tls443Packet(t *testing.T, srcIp string, dstIp string, srcPort int, payload []byte) []byte {
	t.Helper()
	ip := &layers.IPv4{
		Version:  4,
		TTL:      64,
		SrcIP:    net.ParseIP(srcIp).To4(),
		DstIP:    net.ParseIP(dstIp).To4(),
		Protocol: layers.IPProtocolTCP,
	}
	tcp := &layers.TCP{
		SrcPort: layers.TCPPort(srcPort),
		DstPort: 443,
		PSH:     true,
		ACK:     true,
	}
	if err := tcp.SetNetworkLayerForChecksum(ip); err != nil {
		t.Fatal(err)
	}
	buffer := gopacket.NewSerializeBuffer()
	if err := gopacket.SerializeLayers(
		buffer,
		gopacket.SerializeOptions{ComputeChecksums: true, FixLengths: true},
		ip,
		tcp,
		gopacket.Payload(payload),
	); err != nil {
		t.Fatal(err)
	}
	out := make([]byte, len(buffer.Bytes()))
	copy(out, buffer.Bytes())
	return out
}

func TestSniFromClientHello(t *testing.T) {
	name, ech, ok := sniFromClientHello(buildClientHello("Pbs.com"))
	if !ok || name != "pbs.com" || ech {
		t.Fatalf("sniFromClientHello = %q,ech=%v,%v, want pbs.com,false,true (lowercased)", name, ech, ok)
	}

	// an ECH-bearing hello is detected, and the cleartext (outer) name is
	// still returned raw — the recording layer reduces it
	name, ech, ok = sniFromClientHello(buildClientHelloWithEch("Ech-Front.Example-Cdn.co.uk"))
	if !ok || name != "ech-front.example-cdn.co.uk" || !ech {
		t.Fatalf("sniFromClientHello(ech) = %q,ech=%v,%v, want ech-front.example-cdn.co.uk,true,true", name, ech, ok)
	}

	// a ClientHello with no server_name extension yields no SNI
	noSni := buildClientHello("x.example")
	// strip the extensions by finding + truncating is fiddly; instead assert a
	// non-ClientHello record and truncations don't panic or falsely match
	for _, bad := range [][]byte{
		nil,
		{0x16},
		{0x15, 0x03, 0x01, 0x00, 0x00}, // not a handshake record
		noSni[:20],                     // truncated mid-structure
		buildClientHello("bad name!"),  // invalid hostname char → rejected
	} {
		if name, _, ok := sniFromClientHello(bad); ok {
			t.Fatalf("sniFromClientHello(%v-bytes) = %q,true, want ok=false", len(bad), name)
		}
	}
}

func TestRegistrableDomain(t *testing.T) {
	for name, want := range map[string]string{
		// simple eTLD
		"ech-front.example-cdn.com": "example-cdn.com",
		"example-cdn.com":           "example-cdn.com",
		// multi-label eTLD from the public suffix list (not just "last two labels")
		"ech-front.cdn.example-cdn.co.uk": "example-cdn.co.uk",
		// a bare public suffix has no registrable form; unchanged
		"co.uk": "co.uk",
	} {
		if got := registrableDomain(name); got != want {
			t.Fatalf("registrableDomain(%q) = %q, want %q", name, got, want)
		}
	}
}

// TestSniSnifferEchRecordsRegistrableDomain: an ECH-protected hello records the
// outer name reduced to its registrable (eTLD+1) trust domain — never the full
// outer name as if it were the precise destination host. A non-ECH hello still
// records the full name.
func TestSniSnifferEchRecordsRegistrableDomain(t *testing.T) {
	var captured []string
	sniffer := newSniSniffer(func(dstAddr netip.Addr, serverName string) {
		captured = append(captured, serverName)
	})

	// ECH hello with an outer name under a multi-label (co.uk) domain
	sniffer.observe(tls443Packet(t, "10.0.0.5", "93.184.216.34", 40010, buildClientHelloWithEch("ech-front.cdn.example-cdn.co.uk")))
	if !slices.Equal(captured, []string{"example-cdn.co.uk"}) {
		t.Fatalf("captured = %v, want [example-cdn.co.uk] (outer name reduced to eTLD+1)", captured)
	}

	// non-ECH behavior is unchanged: the full name records
	captured = nil
	sniffer.observe(tls443Packet(t, "10.0.0.5", "93.184.216.34", 40011, buildClientHello("host.sub.example-cdn.co.uk")))
	if !slices.Equal(captured, []string{"host.sub.example-cdn.co.uk"}) {
		t.Fatalf("captured = %v, want the full non-ECH name", captured)
	}
}

func TestSniSnifferSingleSegment(t *testing.T) {
	var captured []string
	var capturedAddr netip.Addr
	sniffer := newSniSniffer(func(dstAddr netip.Addr, serverName string) {
		capturedAddr = dstAddr
		captured = append(captured, serverName)
	})

	packet := tls443Packet(t, "10.0.0.5", "93.184.216.34", 40000, buildClientHello("pbs.com"))
	sniffer.observe(packet)

	if !slices.Equal(captured, []string{"pbs.com"}) {
		t.Fatalf("captured = %v, want [pbs.com]", captured)
	}
	if capturedAddr != netip.MustParseAddr("93.184.216.34") {
		t.Fatalf("captured addr = %v, want 93.184.216.34", capturedAddr)
	}
	// no partial reassembly should linger after a complete single-segment hello
	if 0 != sniffer.partialCount.Load() {
		t.Fatalf("partials leaked: %d", sniffer.partialCount.Load())
	}
}

func TestSniSnifferSplitSegments(t *testing.T) {
	var captured []string
	sniffer := newSniSniffer(func(dstAddr netip.Addr, serverName string) {
		captured = append(captured, serverName)
	})

	hello := buildClientHello("split.example.com")
	// split the ClientHello record across two TCP segments of the same flow
	cut := len(hello) / 2
	seg1 := tls443Packet(t, "10.0.0.5", "198.51.100.9", 40001, hello[:cut])
	seg2 := tls443Packet(t, "10.0.0.5", "198.51.100.9", 40001, hello[cut:])

	sniffer.observe(seg1)
	if 0 < len(captured) {
		t.Fatal("SNI captured before the ClientHello was complete")
	}
	if sniffer.partialCount.Load() != 1 {
		t.Fatalf("expected 1 buffered partial after segment 1, got %d", sniffer.partialCount.Load())
	}
	sniffer.observe(seg2)
	if !slices.Equal(captured, []string{"split.example.com"}) {
		t.Fatalf("captured = %v, want [split.example.com]", captured)
	}
	if 0 != sniffer.partialCount.Load() {
		t.Fatalf("partials leaked after completion: %d", sniffer.partialCount.Load())
	}
}

func TestSniSnifferIgnores(t *testing.T) {
	captured := 0
	sniffer := newSniSniffer(func(netip.Addr, string) { captured++ })

	// a bare :443 segment with no ClientHello payload (e.g. an ACK) records nothing
	sniffer.observe(tls443Packet(t, "10.0.0.5", "1.2.3.4", 40003, nil))
	// a :443 segment whose payload isn't a handshake records nothing
	sniffer.observe(tls443Packet(t, "10.0.0.5", "1.2.3.4", 40004, []byte("GET / HTTP/1.1\r\n")))
	// a UDP packet to :443 is not a tls segment (wrong protocol)
	sniffer.observe(testingUdp4Packet("10.0.0.5", "1.2.3.4", 443, buildClientHello("x.example")))

	if captured != 0 {
		t.Fatalf("captured %d names from non-ClientHello traffic, want 0", captured)
	}
	if 0 != sniffer.partialCount.Load() {
		t.Fatalf("partials leaked: %d", sniffer.partialCount.Load())
	}
}

// TestUpgradeMuxSniCapture drives a ClientHello through the mux send path and verifies the
// SNI lands in the reverse index (retrievable via ServerNames) while the packet still
// passes through to the upstream (TLS is observed, never claimed).
func TestUpgradeMuxSniCapture(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &ipMuxRecorder{}
	mux, err := NewUpgradeMux(ctx, TransferPath{}, protocol.ProvideMode_Network, 0, rec.receive, DefaultUpgradeMuxSettings(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	mux.SetUpstream(rec.upstream)

	packet := tls443Packet(t, "10.0.0.5", "93.184.216.34", 40000, buildClientHello("pbs.com"))
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, packet, 0) {
		t.Fatal("443 pass-through returned false")
	}

	if names := mux.ServerNames("93.184.216.34"); !slices.Contains(names, "pbs.com") {
		t.Fatalf("ServerNames(93.184.216.34) = %v, want to contain pbs.com", names)
	}
	// observed, not claimed: the packet reached the upstream
	if sent, _ := rec.counts(); sent != 1 {
		t.Fatalf("the TLS packet was not passed through to the upstream (sent=%d)", sent)
	}
}

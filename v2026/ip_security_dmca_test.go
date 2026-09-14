package connect

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func btHandshake() []byte {
	b := make([]byte, 68)
	copy(b, bittorrentHandshakePrefix)
	return b
}

// a deterministic high-entropy payload: every byte value once => uniform
// distribution, popcount exactly 0.5, ~37% printable. Looks like ciphertext.
func encryptedPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}

func tlsClientHello() []byte {
	return []byte{0x16, 0x03, 0x01, 0x00, 0x2a, 0x01, 0x00, 0x00, 0x26}
}

func dtlsClientHello() []byte {
	// 13-byte record header (type, version, epoch, seq, length) then ClientHello(0x01)
	return []byte{0x16, 0xfe, 0xfd, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01}
}

func quicInitial() []byte {
	return []byte{0xc0, 0x00, 0x00, 0x00, 0x01, 0x08, 0xde, 0xad, 0xbe, 0xef}
}

func stunBinding() []byte {
	b := make([]byte, 20)
	b[0], b[1] = 0x00, 0x01 // binding request
	b[2], b[3] = 0x00, 0x00 // length 0
	b[4], b[5], b[6], b[7] = 0x21, 0x12, 0xa4, 0x42
	return b
}

func TestDmcaBittorrentSignatures(t *testing.T) {
	dhtPing := []byte("d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe")
	tracker := append(append([]byte{}, udpTrackerConnectMagic...), 0, 0, 0, 0, 0x12, 0x34, 0x56, 0x78)
	httpTracker := []byte("GET /announce?info_hash=%01%02%03&peer_id=x HTTP/1.1\r\nHost: t\r\n\r\n")
	utp := append(append([]byte{0x01, 0x00}, make([]byte, 18)...), btHandshake()...)

	cases := []struct {
		name  string
		proto IpProtocol
		b     []byte
		want  bool
	}{
		{"tcp handshake", IpProtocolTcp, btHandshake(), true},
		{"tcp http tracker", IpProtocolTcp, httpTracker, true},
		{"udp dht ping", IpProtocolUdp, dhtPing, true},
		{"udp tracker connect", IpProtocolUdp, tracker, true},
		{"udp utp+handshake", IpProtocolUdp, utp, true},
		{"tcp tls not bt", IpProtocolTcp, tlsClientHello(), false},
		{"udp dns not bt", IpProtocolUdp, []byte("\x12\x34\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00"), false},
		{"udp random not bt", IpProtocolUdp, encryptedPayload(256), false},
	}
	for _, c := range cases {
		ipPath := &IpPath{Protocol: c.proto}
		if got := detectBittorrentSignature(ipPath, c.b); got != c.want {
			t.Errorf("%s: detectBittorrentSignature = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestDmcaEncryptedHeuristic(t *testing.T) {
	settings := DefaultDmcaSecurityPolicySettings()
	if !payloadLooksEncrypted(encryptedPayload(256), settings) {
		t.Fatal("uniform random payload should look encrypted")
	}
	text := []byte("GET / HTTP/1.1\r\nHost: example.com\r\nUser-Agent: test\r\n\r\n")
	if payloadLooksEncrypted(text, settings) {
		t.Fatal("plaintext http should not look encrypted")
	}
	if payloadLooksEncrypted(encryptedPayload(8), settings) {
		t.Fatal("payload below min length should be inconclusive")
	}
}

func dmcaPath(proto IpProtocol, sport int, dport int, syn bool) *IpPath {
	return &IpPath{
		Version:         4,
		Protocol:        proto,
		SourceIp:        net.ParseIP("10.0.0.2"),
		SourcePort:      sport,
		DestinationIp:   net.ParseIP("8.8.8.8"),
		DestinationPort: dport,
		Syn:             syn,
	}
}

// plaintext bittorrent handshake -> incident on the first data packet
func TestDmcaStateMachineHandshakeIncident(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultDmcaSecurityPolicySettings()
	web := newWebStandardDetector(DefaultWebStandardSettings())

	d := newDmcaDetector(ctx, settings, web)
	if r := d.inspect(dmcaPath(IpProtocolTcp, 40001, 51413, false), btHandshake()); r != SecurityPolicyResultIncident {
		t.Fatalf("handshake -> %v, want incident", r)
	}
}

// encrypted, non-web-standard TCP observed from SYN -> drop after the budget
func TestDmcaStateMachineEncryptedTcpDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultDmcaSecurityPolicySettings()
	web := newWebStandardDetector(DefaultWebStandardSettings())

	d := newDmcaDetector(ctx, settings, web)
	path := dmcaPath(IpProtocolTcp, 40002, 50000, true)
	if r := d.inspect(path, nil); r != SecurityPolicyResultAllow {
		t.Fatalf("syn -> %v, want allow (inspecting)", r)
	}
	data := dmcaPath(IpProtocolTcp, 40002, 50000, false)
	var last SecurityPolicyResult
	for i := 0; i < settings.EncryptedDecisionPackets; i += 1 {
		last = d.inspect(data, encryptedPayload(512))
	}
	if last != SecurityPolicyResultDrop {
		t.Fatalf("encrypted tcp from syn -> %v, want drop", last)
	}
}

// same bytes, but joined mid-stream (no SYN seen) -> must NOT drop
func TestDmcaStateMachineEncryptedTcpMidstreamAllowed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultDmcaSecurityPolicySettings()
	web := newWebStandardDetector(DefaultWebStandardSettings())

	d := newDmcaDetector(ctx, settings, web)
	data := dmcaPath(IpProtocolTcp, 40003, 50000, false)
	var last SecurityPolicyResult
	for i := 0; i < settings.InspectionPacketBudget+1; i += 1 {
		last = d.inspect(data, encryptedPayload(512))
	}
	if last != SecurityPolicyResultAllow {
		t.Fatalf("encrypted tcp mid-stream -> %v, want allow", last)
	}
}

func TestDmcaFreshSynReplacesTerminalVerdict(t *testing.T) {
	settings := DefaultDmcaSecurityPolicySettings()
	detector := newDmcaDetector(
		nil,
		settings,
		newWebStandardDetector(DefaultWebStandardSettings()),
	)

	firstSyn := dmcaPath(IpProtocolTcp, 40013, 50000, true)
	firstSyn.SequenceNumber = 1000
	if verdict := detector.classify(firstSyn, nil); verdict != dmcaInspecting {
		t.Fatalf("first SYN verdict = %d, want inspecting", verdict)
	}
	firstData := dmcaPath(IpProtocolTcp, 40013, 50000, false)
	firstData.SequenceNumber = 1001
	if verdict := detector.classify(firstData, []byte("GET / HTTP/1.1\r\n\r\n")); verdict != dmcaAllow {
		t.Fatalf("first connection verdict = %d, want allow", verdict)
	}

	secondSyn := dmcaPath(IpProtocolTcp, 40013, 50000, true)
	secondSyn.SequenceNumber = 2000
	if verdict := detector.classify(secondSyn, nil); verdict != dmcaInspecting {
		t.Fatalf("reused-tuple SYN verdict = %d, want inspecting", verdict)
	}
	secondData := dmcaPath(IpProtocolTcp, 40013, 50000, false)
	secondData.SequenceNumber = 2001
	if verdict := detector.classify(secondData, btHandshake()); verdict != dmcaBittorrent {
		t.Fatalf("second connection verdict = %d, want bittorrent", verdict)
	}
}

func TestDmcaNamespacesIdenticalFlowBySenderClientId(t *testing.T) {
	detector := newDmcaDetector(
		nil,
		DefaultDmcaSecurityPolicySettings(),
		newWebStandardDetector(DefaultWebStandardSettings()),
	)
	firstSenderClientId := NewId()
	secondSenderClientId := NewId()
	path := dmcaPath(IpProtocolTcp, 40016, 50000, false)

	if verdict := detector.classifyForSender(
		firstSenderClientId,
		path,
		[]byte("GET / HTTP/1.1\r\n\r\n"),
	); verdict != dmcaAllow {
		t.Fatalf("first sender verdict = %d, want allow", verdict)
	}
	if verdict := detector.classifyForSender(
		secondSenderClientId,
		path,
		btHandshake(),
	); verdict != dmcaBittorrent {
		t.Fatalf("second sender inherited first sender verdict: got %d, want bittorrent", verdict)
	}
	if flowCount := detector.flowCount(); flowCount != 2 {
		t.Fatalf("identical tuple across senders retained %d flows, want 2", flowCount)
	}

	returnFin := path.Reverse()
	returnFin.Fin = true
	detector.touchIngressForSender(firstSenderClientId, returnFin)
	if flowCount := detector.flowCount(); flowCount != 1 {
		t.Fatalf("first sender return FIN left %d flows, want second sender only", flowCount)
	}
	if verdict := detector.classifyForSender(secondSenderClientId, path, nil); verdict != dmcaBittorrent {
		t.Fatalf("first sender teardown changed second sender verdict to %d", verdict)
	}
	detector.retireEgressForSender(secondSenderClientId, path)
	if flowCount := detector.flowCount(); flowCount != 0 {
		t.Fatalf("provider flow close left %d sender-scoped flows, want 0", flowCount)
	}
}

func TestProviderReversePolicyCarriesSenderClientId(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := Reverse(DefaultSecurityPolicy(ctx))
	firstSenderClientId := NewId()
	secondSenderClientId := NewId()
	path := dmcaPath(IpProtocolTcp, 40017, 50000, false)

	firstResult, err := inspectAndRefreshIngressForSenderBorrowed(
		policy,
		firstSenderClientId,
		protocol.ProvideMode_Public,
		*path,
		[]byte("GET / HTTP/1.1\r\n\r\n"),
	)
	if err != nil || firstResult != SecurityPolicyResultAllow {
		t.Fatalf("first provider sender result = (%d, %v), want allow", firstResult, err)
	}
	secondResult, err := inspectAndRefreshIngressForSenderBorrowed(
		policy,
		secondSenderClientId,
		protocol.ProvideMode_Public,
		*path,
		btHandshake(),
	)
	if err != nil || secondResult != SecurityPolicyResultIncident {
		t.Fatalf("second provider sender result = (%d, %v), want incident", secondResult, err)
	}
}

func TestDmcaTcpTeardownClearsFlowState(t *testing.T) {
	detector := newDmcaDetector(
		nil,
		DefaultDmcaSecurityPolicySettings(),
		newWebStandardDetector(DefaultWebStandardSettings()),
	)

	finPath := dmcaPath(IpProtocolTcp, 40014, 50000, true)
	finPath.SequenceNumber = 3000
	detector.classify(finPath, nil)
	finPath.Syn = false
	finPath.Fin = true
	finPath.SequenceNumber = 3001
	detector.classify(finPath, nil)

	rstPath := dmcaPath(IpProtocolTcp, 40015, 50000, true)
	rstPath.SequenceNumber = 4000
	detector.classify(rstPath, nil)
	rstPath.Syn = false
	rstPath.Rst = true
	rstPath.SequenceNumber = 4001
	detector.classify(rstPath, nil)

	if flowCount := detector.flowCount(); flowCount != 0 {
		t.Fatalf("DMCA flow table retained %d TCP teardown states, want 0", flowCount)
	}
}

// encrypted UDP (first datagram is the start) -> drop
func TestDmcaStateMachineEncryptedUdpDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultDmcaSecurityPolicySettings()
	web := newWebStandardDetector(DefaultWebStandardSettings())

	d := newDmcaDetector(ctx, settings, web)
	path := dmcaPath(IpProtocolUdp, 40004, 50000, false)
	var last SecurityPolicyResult
	for i := 0; i < settings.EncryptedDecisionPackets; i += 1 {
		last = d.inspect(path, encryptedPayload(512))
	}
	if last != SecurityPolicyResultDrop {
		t.Fatalf("encrypted udp -> %v, want drop", last)
	}
}

// whitelisted web standard that looks encrypted on later packets stays allowed
func TestDmcaStateMachineQuicAllowed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultDmcaSecurityPolicySettings()
	web := newWebStandardDetector(DefaultWebStandardSettings())

	d := newDmcaDetector(ctx, settings, web)
	path := dmcaPath(IpProtocolUdp, 40005, 50000, false)
	if r := d.inspect(path, quicInitial()); r != SecurityPolicyResultAllow {
		t.Fatalf("quic initial -> %v, want allow", r)
	}
	// subsequent encrypted 1-RTT packets must remain allowed (terminal)
	if r := d.inspect(path, encryptedPayload(512)); r != SecurityPolicyResultAllow {
		t.Fatalf("quic post-handshake -> %v, want allow", r)
	}
}

// an unknown plaintext protocol is allowed (only encrypted non-web is dropped)
func TestDmcaStateMachinePlaintextAllowed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultDmcaSecurityPolicySettings()
	web := newWebStandardDetector(DefaultWebStandardSettings())

	d := newDmcaDetector(ctx, settings, web)
	path := dmcaPath(IpProtocolUdp, 40006, 50000, false)
	if r := d.inspect(path, []byte("PLAINTEXT GAME PROTOCOL HELLO v1 ............")); r != SecurityPolicyResultAllow {
		t.Fatalf("plaintext -> %v, want allow", r)
	}
}

// TestDmcaRawHttpShortRequestAllowed: a short HTTP request line (below MinEncryptedPayload)
// observed from SYN is allowed immediately and terminally — so a later high-entropy body on
// the same flow cannot trip the encrypted-traffic heuristic. Without the explicit HTTP
// detection the short request would stay inspecting and be dropped once the high-entropy
// packets arrived.
func TestDmcaRawHttpShortRequestAllowed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultDmcaSecurityPolicySettings()
	d := newDmcaDetector(ctx, settings, newWebStandardDetector(DefaultWebStandardSettings()))
	if r := d.inspect(dmcaPath(IpProtocolTcp, 40010, 40000, true), nil); r != SecurityPolicyResultAllow {
		t.Fatalf("syn -> %v, want allow", r)
	}
	data := dmcaPath(IpProtocolTcp, 40010, 40000, false)
	if r := d.inspect(data, []byte("GET /a HTTP/1.1\r\n\r\n")); r != SecurityPolicyResultAllow {
		t.Fatalf("short http get -> %v, want allow", r)
	}
	for i := 0; i < settings.EncryptedDecisionPackets+1; i += 1 {
		if r := d.inspect(data, encryptedPayload(512)); r != SecurityPolicyResultAllow {
			t.Fatalf("post-http high-entropy -> %v, want allow (terminal)", r)
		}
	}
}

// TestDmcaHttpTrackerIsBittorrent: an HTTP-tracker GET (info_hash + /announce) is still
// BitTorrent, not allowed as raw HTTP — the HTTP check runs after the BitTorrent signatures.
func TestDmcaHttpTrackerIsBittorrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newDmcaDetector(ctx, DefaultDmcaSecurityPolicySettings(), newWebStandardDetector(DefaultWebStandardSettings()))
	tracker := []byte("GET /announce?info_hash=%01%02&peer_id=x HTTP/1.1\r\nHost: t\r\n\r\n")
	if r := d.inspect(dmcaPath(IpProtocolTcp, 40012, 40000, false), tracker); r != SecurityPolicyResultIncident {
		t.Fatalf("http tracker -> %v, want incident", r)
	}
}

// TestEgressAllowsRawHttpNonStandardPort: end-to-end through the egress policy, CFAA passes
// the ephemeral port to DPI and DPI allows the raw HTTP request.
func TestEgressAllowsRawHttpNonStandardPort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := DefaultSecurityPolicy(ctx)
	get := []byte("GET /stream/audio.mp3 HTTP/1.1\r\nHost: stream.example.com\r\n\r\n")
	r, err := policy.InspectEgress(protocol.ProvideMode_Public, dmcaPath(IpProtocolTcp, 41010, 40000, false), get)
	if err != nil {
		t.Fatal(err)
	}
	if r != SecurityPolicyResultAllow {
		t.Fatalf("raw http on :40000 -> %v, want allow", r)
	}
}

func TestIsHttpRequest(t *testing.T) {
	yes := [][]byte{
		[]byte("GET / HTTP/1.1\r\n"),
		[]byte("POST /x HTTP/1.0\r\n"),
		[]byte("HEAD /y HTTP/1.1\r\nHost: a\r\n"),
	}
	no := [][]byte{
		[]byte("GET /no-version\r\n"),
		[]byte("CONNECT host:443 HTTP/1.1\r\n"), // opaque tunnel, not allowed as raw http
		tlsClientHello(),
		[]byte("random bytes without a method"),
	}
	for _, b := range yes {
		if !isHttpRequest(b) {
			t.Errorf("isHttpRequest(%q) = false, want true", b)
		}
	}
	for _, b := range no {
		if isHttpRequest(b) {
			t.Errorf("isHttpRequest(%q) = true, want false", b)
		}
	}
}

func TestEgressSecurityPolicyDpi(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := DefaultSecurityPolicy(ctx)

	// bittorrent handshake on a non-bittorrent port (all-ports coverage) -> incident
	r, err := policy.InspectEgress(protocol.ProvideMode_Public, dmcaPath(IpProtocolTcp, 41001, 51413, false), btHandshake())
	if err != nil {
		t.Fatal(err)
	}
	if r != SecurityPolicyResultIncident {
		t.Fatalf("handshake on 51413 -> %v, want incident", r)
	}

	// tls on a non-standard (>=1024) port -> allow via web-standard detection
	r, _ = policy.InspectEgress(protocol.ProvideMode_Public, dmcaPath(IpProtocolTcp, 41002, 8443, false), tlsClientHello())
	if r != SecurityPolicyResultAllow {
		t.Fatalf("tls on 8443 -> %v, want allow", r)
	}

	// a privileged destination port (<1024) is allowed without inspection; even a BitTorrent
	// handshake (an incident on a high port) passes
	r, _ = policy.InspectEgress(protocol.ProvideMode_Public, dmcaPath(IpProtocolTcp, 41005, 443, false), btHandshake())
	if r != SecurityPolicyResultAllow {
		t.Fatalf("bittorrent handshake on privileged port 443 -> %v, want allow", r)
	}

	// known bittorrent port -> drop without payload
	r, _ = policy.InspectEgress(protocol.ProvideMode_Public, dmcaPath(IpProtocolTcp, 41003, 6881, false), nil)
	if r != SecurityPolicyResultDrop {
		t.Fatalf("port 6881 -> %v, want drop", r)
	}

	// network-mode traffic bypasses inspection entirely
	r, _ = policy.InspectEgress(protocol.ProvideMode_Network, dmcaPath(IpProtocolTcp, 41004, 51413, false), btHandshake())
	if r != SecurityPolicyResultAllow {
		t.Fatalf("network-mode handshake -> %v, want allow", r)
	}
}

// TestDmcaFlowTtl: a return-direction packet refreshes a tracked flow's activity (touch), an
// active flow is not evicted, and the idle scan evicts a flow once it is idle past FlowTtl.
func TestDmcaFlowTtl(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultDmcaSecurityPolicySettings()
	settings.FlowTtl = 50 * time.Millisecond
	d := newDmcaDetector(ctx, settings, newWebStandardDetector(DefaultWebStandardSettings()))

	// an egress flow to a non-privileged destination is tracked
	eg := dmcaPath(IpProtocolTcp, 41100, 40000, false)
	d.classify(eg, []byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))

	key := dmcaFlowKeyForPath(Id{}, eg)
	shard := d.shards[dmcaShardIndex(key)]
	read := func() (*dmcaFlowState, bool) {
		shard.mu.RLock()
		defer shard.mu.RUnlock()
		st, ok := shard.flows[key]
		return st, ok
	}
	flowCount := func() int {
		shard.mu.RLock()
		defer shard.mu.RUnlock()
		return len(shard.flows)
	}

	st, ok := read()
	if !ok {
		t.Fatal("egress flow not tracked")
	}
	t0 := st.LastActivityTime()

	// a sent (egress) packet refreshes the flow's activity
	time.Sleep(10 * time.Millisecond)
	d.touchEgress(eg)
	t1 := st.LastActivityTime()
	if !t1.After(t0) {
		t.Fatal("touchEgress did not refresh flow activity")
	}

	// a return-direction (ingress) packet also refreshes it, reversing the 5-tuple to the key
	time.Sleep(10 * time.Millisecond)
	d.touchIngress(eg.Reverse())
	if !st.LastActivityTime().After(t1) {
		t.Fatal("touchIngress did not refresh flow activity")
	}

	// a refresh for an untracked 5-tuple is a no-op (no panic, no new entry), in either direction
	d.touchEgress(dmcaPath(IpProtocolTcp, 9999, 40000, false))
	d.touchIngress(dmcaPath(IpProtocolTcp, 9999, 40000, false).Reverse())
	if n := flowCount(); n != 1 {
		t.Fatalf("refresh of an untracked flow changed the table: got %d flows, want 1", n)
	}

	// a recently-active flow is not evicted
	d.evictIdle(time.Now())
	if _, ok := read(); !ok {
		t.Fatal("active flow was evicted")
	}

	// the idle scan evicts the flow once it is idle past FlowTtl
	d.evictIdle(time.Now().Add(time.Second))
	if _, ok := read(); ok {
		t.Fatal("idle flow not evicted past FlowTtl")
	}
}

// TestReverseSecurityPolicy: the provider's Reverse(client policy) applies the client's egress DPI
// on its ingress (the remote client's outbound, received from the tunnel) and the client's ingress
// source check on its egress (the return into the tunnel).
func TestReverseSecurityPolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rev := Reverse(DefaultSecurityPolicy(ctx))

	// reversed ingress == client egress DPI: a bittorrent handshake is reported as an incident
	r, err := rev.InspectIngress(protocol.ProvideMode_Public, dmcaPath(IpProtocolTcp, 41020, 51413, false), btHandshake())
	if err != nil {
		t.Fatal(err)
	}
	if r != SecurityPolicyResultIncident {
		t.Fatalf("reversed ingress (=client egress DPI) on bittorrent -> %v, want incident", r)
	}

	// reversed egress == client ingress source check: a blocked source port is dropped
	r, _ = rev.InspectEgress(protocol.ProvideMode_Public, dmcaPath(IpProtocolTcp, 6881, 40000, false), nil)
	if r != SecurityPolicyResultDrop {
		t.Fatalf("reversed egress (=client ingress source check) on blocked source port -> %v, want drop", r)
	}

	// network-mode bypasses, same as the underlying policy
	r, _ = rev.InspectIngress(protocol.ProvideMode_Network, dmcaPath(IpProtocolTcp, 41021, 51413, false), btHandshake())
	if r != SecurityPolicyResultAllow {
		t.Fatalf("reversed network-mode -> %v, want allow", r)
	}
}

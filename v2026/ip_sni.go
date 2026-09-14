package connect

// Passive TLS SNI capture for egress TCP/443 flows: the mux observes the ClientHello
// of each new HTTPS flow, extracts the Server Name Indication, and records the
// destination ip -> server name into the reverse index (firing the learned callbacks
// that invalidate stale block-action decisions). It never claims a flow — the packet
// always passes through unchanged — so it is a pure enrichment: it names flows the DNS
// path can't (a client dialing from its own long-TTL DNS cache emits no query the mux
// sees, so without this the block-action feed and ServerName path affinity fall back to
// the bare ip). ClientHellos split across TCP segments are reassembled in a bounded
// buffer. An ECH-protected ClientHello still carries a cleartext OUTER server_name (the
// client-facing server): ECH hides the exact destination hostname but not the trust
// domain, so when the encrypted_client_hello extension is present the outer name is
// reduced to its registrable (eTLD+1) domain before recording — the destination
// controls that domain, but the outer name is NEVER recorded as the precise host.

import (
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/publicsuffix"
)

const (
	tlsRecordTypeHandshake  = 0x16
	tlsVersionMajor         = 0x03
	tlsHandshakeClientHello = 0x01
	tlsExtensionServerName  = 0x0000
	// encrypted_client_hello (draft-ietf-tls-esni). Its presence means the
	// cleartext server_name is the OUTER (client-facing) name, not the
	// destination host.
	tlsExtensionEncryptedClientHello = 0xfe0d
	tlsSniTypeHostName               = 0x00

	// sniMaxClientHelloBytes caps the bytes buffered per flow while reassembling a
	// split ClientHello. A TLS record can be up to 16KB, but a real ClientHello —
	// even with post-quantum key shares + ECH — is a few KB; cap below the record
	// max so a malformed/hostile length can't buffer unbounded.
	sniMaxClientHelloBytes = 8 * 1024
	// sniMaxFlows hard-caps concurrent partial-ClientHello reassemblies (the memory
	// bound: sniMaxFlows * sniMaxClientHelloBytes worst case). Partials are transient
	// (a ClientHello completes within a round trip), so this is a DoS ceiling, not a
	// working-set size.
	sniMaxFlows = 128
	// sniFlowTtl bounds how long a partial reassembly is retained: a flow whose
	// ClientHello never completes (dropped / reset) is dropped after this.
	sniFlowTtl = 10 * time.Second
)

// sniFlowKey identifies a client TCP flow (the 4-tuple) for ClientHello reassembly.
type sniFlowKey struct {
	srcAddr netip.Addr
	srcPort uint16
	dstAddr netip.Addr
	dstPort uint16
}

// tlsSegment is a TCP/443 segment's flow identity and payload, extracted from a raw
// packet without allocation. payload aliases the packet buffer — copy before retaining.
type tlsSegment struct {
	flow    sniFlowKey
	payload []byte
}

type sniPartial struct {
	buf        []byte
	total      int // the full TLS record length (5 + record body), the reassembly target
	firstNanos int64
}

// sniSniffer extracts the TLS SNI from the ClientHello of egress TCP/443 flows and
// records dst_ip -> server_name via record. Self-contained + independently testable: it
// holds no tun/mux, reassembles split ClientHellos in a bounded buffer, and never
// retains a reference to a packet buffer.
type sniSniffer struct {
	// record is called with a captured (dstAddr, serverName), outside the sniffer lock.
	record func(dstAddr netip.Addr, serverName string)

	lock     sync.Mutex
	partials map[sniFlowKey]*sniPartial
	// partialCount mirrors len(partials) for a lock-free fast path: a non-ClientHello
	// segment with no reassembly in flight is ignored without taking the lock.
	partialCount atomic.Int64
}

func newSniSniffer(record func(dstAddr netip.Addr, serverName string)) *sniSniffer {
	return &sniSniffer{
		record:   record,
		partials: map[sniFlowKey]*sniPartial{},
	}
}

// observe inspects one egress TCP/443 packet: if it starts (or continues) a ClientHello,
// the SNI is extracted once the record is complete and recorded. Never claims the flow.
// observe parses an egress packet and, if it is TCP/443, feeds it to the reassembler. The
// mux hot path avoids this second parse by calling observeSegment directly with the segment
// peekClaim already extracted; observe remains for callers (tests) that start from a packet.
func (self *sniSniffer) observe(packet []byte) {
	if seg, ok := peekTlsSegment(packet); ok {
		self.observeSegment(seg)
	}
}

// observeSegment reassembles a TCP/443 flow toward a complete ClientHello and records the SNI
// name when one is recovered. seg is the already-extracted flow 4-tuple and TCP payload.
func (self *sniSniffer) observeSegment(seg tlsSegment) {
	total, isHello := clientHelloRecordLen(seg.payload)
	// fast path: a non-ClientHello segment is only interesting as a continuation of a
	// flow already being reassembled; skip the lock when nothing is pending
	if !isHello && self.partialCount.Load() == 0 {
		return
	}

	var name string
	var dstAddr netip.Addr
	func() {
		self.lock.Lock()
		defer self.lock.Unlock()

		partial := self.partials[seg.flow]
		if partial != nil && sniExpired(partial.firstNanos) {
			delete(self.partials, seg.flow)
			self.partialCount.Store(int64(len(self.partials)))
			partial = nil
		}
		if partial == nil && !isHello {
			// a continuation for a flow we aren't reassembling (or an expired one)
			return
		}

		var record []byte
		var recordTotal int
		if partial != nil {
			// continuation: append, bounded by the per-flow cap
			if room := sniMaxClientHelloBytes - len(partial.buf); 0 < room {
				add := seg.payload
				if room < len(add) {
					add = add[:room]
				}
				partial.buf = append(partial.buf, add...)
			}
			record = partial.buf
			recordTotal = partial.total
		} else {
			record = seg.payload
			recordTotal = total
		}

		if len(record) < recordTotal && len(record) < sniMaxClientHelloBytes {
			// the record isn't complete yet — buffer this flow for the continuation
			if partial == nil {
				if sniMaxFlows <= len(self.partials) {
					self.evictOldestLocked()
				}
				buf := make([]byte, len(seg.payload))
				copy(buf, seg.payload)
				self.partials[seg.flow] = &sniPartial{
					buf:        buf,
					total:      total,
					firstNanos: time.Now().UnixNano(),
				}
				self.partialCount.Store(int64(len(self.partials)))
			}
			return
		}

		// the record is complete (or hit the cap): parse it and retire the flow
		if partial != nil {
			delete(self.partials, seg.flow)
			self.partialCount.Store(int64(len(self.partials)))
		}
		parseLen := recordTotal
		if len(record) < parseLen {
			parseLen = len(record)
		}
		var ech bool
		name, ech, _ = sniFromClientHello(record[:parseLen])
		if ech && name != "" {
			// ECH: the cleartext name is the OUTER (client-facing) server
			// name. The exact destination host is encrypted; the outer name
			// must still be under a domain the destination controls, so
			// record only its registrable (eTLD+1) trust domain and never
			// the outer name as if it were the precise destination host.
			name = registrableDomain(name)
		}
		dstAddr = seg.flow.dstAddr
	}()

	// record outside the lock: it fires the reverse index's learned callbacks, which
	// reach into the multi-client — never hold the sniffer lock across that
	if name != "" {
		self.record(dstAddr, name)
	}
}

func sniExpired(firstNanos int64) bool {
	return int64(sniFlowTtl) < time.Now().UnixNano()-firstNanos
}

// evictOldestLocked drops the least-recently-started partial reassembly (approximate:
// the one that has waited longest for its continuation, most likely abandoned).
func (self *sniSniffer) evictOldestLocked() {
	var oldestKey sniFlowKey
	var oldestNanos int64
	found := false
	for key, partial := range self.partials {
		if !found || partial.firstNanos < oldestNanos {
			oldestKey = key
			oldestNanos = partial.firstNanos
			found = true
		}
	}
	if found {
		delete(self.partials, oldestKey)
		self.partialCount.Store(int64(len(self.partials)))
	}
}

// shed drops all in-flight reassemblies under host memory pressure. Captured names are
// already recorded in the reverse index; only the transient partials are released.
func (self *sniSniffer) shed() {
	self.lock.Lock()
	defer self.lock.Unlock()
	clear(self.partials)
	self.partialCount.Store(0)
}

// peekTlsSegment extracts the flow 4-tuple and TCP payload of an egress TCP/443 packet
// from the header offsets, without allocating. ok is false for anything that isn't a
// well-formed TCP/443 packet. IPv6 extension headers are walked (ip_ipv6_ext.go); a v6
// fragment has no transport header to read and is not classified. The returned payload
// aliases packet.
func peekTlsSegment(packet []byte) (tlsSegment, bool) {
	if len(packet) < 20 {
		return tlsSegment{}, false
	}
	var ipHeaderLen int
	var ipPayloadEnd int
	var srcAddr, dstAddr netip.Addr
	switch packet[0] >> 4 {
	case 4:
		ihl := int(packet[0]&0x0f) * 4
		if ihl < 20 || len(packet) < ihl {
			return tlsSegment{}, false
		}
		if packet[9] != 6 { // not TCP
			return tlsSegment{}, false
		}
		totalLen := int(packet[2])<<8 | int(packet[3])
		if totalLen < ihl || len(packet) < totalLen {
			// a truncated capture or a bad length: bound to what we actually have
			totalLen = len(packet)
		}
		srcAddr, _ = netip.AddrFromSlice(packet[12:16])
		dstAddr, _ = netip.AddrFromSlice(packet[16:20])
		ipHeaderLen = ihl
		ipPayloadEnd = totalLen
	case 6:
		nextHeader, transportOffset, end, ok := ipv6TransportOffset(packet)
		if !ok || nextHeader != ipProtocolNumberTcp {
			return tlsSegment{}, false
		}
		srcAddr, _ = netip.AddrFromSlice(packet[8:24])
		dstAddr, _ = netip.AddrFromSlice(packet[24:40])
		ipHeaderLen = transportOffset
		ipPayloadEnd = end
	default:
		return tlsSegment{}, false
	}

	return tcpSegment443(packet, ipHeaderLen, ipPayloadEnd, srcAddr, dstAddr)
}

// tcpSegment443 builds the flow 4-tuple and TCP payload from a packet whose IP header has
// already been parsed: ipHeaderLen is the TCP header offset, ipPayloadEnd bounds the L4 data,
// and srcAddr/dstAddr are the parsed IP addresses. ok is false unless this is a well-formed
// TCP segment to port 443. The returned payload aliases packet. This is the shared tail of
// both peekTlsSegment (which parses the IP header itself) and the mux's peekClaim (which has
// already parsed it while classifying), so a TCP/443 packet's headers are walked only once.
func tcpSegment443(packet []byte, ipHeaderLen, ipPayloadEnd int, srcAddr, dstAddr netip.Addr) (tlsSegment, bool) {
	tcp := packet[ipHeaderLen:ipPayloadEnd]
	if len(tcp) < 20 {
		return tlsSegment{}, false
	}
	dstPort := uint16(tcp[2])<<8 | uint16(tcp[3])
	if dstPort != 443 {
		return tlsSegment{}, false
	}
	dataOffset := int(tcp[12]>>4) * 4
	if dataOffset < 20 || len(tcp) < dataOffset {
		return tlsSegment{}, false
	}
	srcPort := uint16(tcp[0])<<8 | uint16(tcp[1])
	return tlsSegment{
		flow: sniFlowKey{
			srcAddr: srcAddr.Unmap(),
			srcPort: srcPort,
			dstAddr: dstAddr.Unmap(),
			dstPort: dstPort,
		},
		payload: tcp[dataOffset:],
	}, true
}

// clientHelloRecordLen reports whether payload begins a TLS handshake record carrying a
// ClientHello, and if so the full record length (5-byte record header + body) — the
// target size for reassembly.
func clientHelloRecordLen(payload []byte) (total int, ok bool) {
	if len(payload) < 6 {
		return 0, false
	}
	if payload[0] != tlsRecordTypeHandshake || payload[1] != tlsVersionMajor {
		return 0, false
	}
	if payload[5] != tlsHandshakeClientHello {
		return 0, false
	}
	recordBodyLen := int(payload[3])<<8 | int(payload[4])
	return 5 + recordBodyLen, true
}

// sniFromClientHello walks a (complete) TLS ClientHello record and returns the SNI
// host_name plus whether the hello carries the encrypted_client_hello extension (in
// which case the name is the OUTER server name — see the header comment). ok is false
// when there is no server_name extension, the hostname is not a plausible host, or the
// record is malformed/truncated. Every field length is bounds checked against the
// remaining bytes — the input is attacker-controlled.
func sniFromClientHello(record []byte) (name string, ech bool, ok bool) {
	// 5-byte record header, then the handshake message
	if len(record) < 9 || record[0] != tlsRecordTypeHandshake {
		return "", false, false
	}
	b := record[5:]
	if b[0] != tlsHandshakeClientHello {
		return "", false, false
	}
	hsLen := int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	b = b[4:]
	if len(b) > hsLen {
		b = b[:hsLen]
	}
	// client_version (2) + random (32)
	if len(b) < 34 {
		return "", false, false
	}
	b = b[34:]
	// session_id: 1-byte length + id
	if len(b) < 1 {
		return "", false, false
	}
	sessionIdLen := int(b[0])
	b = b[1:]
	if len(b) < sessionIdLen {
		return "", false, false
	}
	b = b[sessionIdLen:]
	// cipher_suites: 2-byte length + suites
	if len(b) < 2 {
		return "", false, false
	}
	cipherLen := int(b[0])<<8 | int(b[1])
	b = b[2:]
	if len(b) < cipherLen {
		return "", false, false
	}
	b = b[cipherLen:]
	// compression_methods: 1-byte length + methods
	if len(b) < 1 {
		return "", false, false
	}
	compressionLen := int(b[0])
	b = b[1:]
	if len(b) < compressionLen {
		return "", false, false
	}
	b = b[compressionLen:]
	// extensions: 2-byte length + extensions (absent in SSLv3-style hellos)
	if len(b) < 2 {
		return "", false, false
	}
	extensionsLen := int(b[0])<<8 | int(b[1])
	b = b[2:]
	if len(b) > extensionsLen {
		b = b[:extensionsLen]
	}
	// walk EVERY extension: the encrypted_client_hello extension may come
	// before or after server_name, and both must be seen
	for len(b) >= 4 {
		extType := int(b[0])<<8 | int(b[1])
		extLen := int(b[2])<<8 | int(b[3])
		b = b[4:]
		if len(b) < extLen {
			// truncated extension list: keep a name already found (matching
			// the previous first-win behavior), reject when the server_name
			// extension was not yet reached
			if ok {
				break
			}
			return "", false, false
		}
		extData := b[:extLen]
		b = b[extLen:]
		switch extType {
		case tlsExtensionServerName:
			if !ok {
				name, ok = sniFromServerNameExtension(extData)
			}
		case tlsExtensionEncryptedClientHello:
			ech = true
		}
	}
	if !ok {
		return "", false, false
	}
	return name, ech, true
}

// registrableDomain reduces a hostname to its registrable (effective-TLD+1)
// domain per the public suffix list, e.g. "ech-front.example-cdn.co.uk" ->
// "example-cdn.co.uk". A name that is itself a public suffix (or otherwise
// has no registrable form) is returned unchanged — it already identifies no
// specific host.
func registrableDomain(name string) string {
	if domain, err := publicsuffix.EffectiveTLDPlusOne(name); err == nil {
		return domain
	}
	return name
}

// sniFromServerNameExtension parses a server_name extension body and returns the first
// host_name entry.
func sniFromServerNameExtension(data []byte) (string, bool) {
	// ServerNameList: 2-byte list length, then entries
	if len(data) < 2 {
		return "", false
	}
	listLen := int(data[0])<<8 | int(data[1])
	data = data[2:]
	if len(data) > listLen {
		data = data[:listLen]
	}
	for len(data) >= 3 {
		nameType := data[0]
		nameLen := int(data[1])<<8 | int(data[2])
		data = data[3:]
		if len(data) < nameLen {
			return "", false
		}
		name := data[:nameLen]
		data = data[nameLen:]
		if nameType == tlsSniTypeHostName {
			return normalizeSni(name)
		}
	}
	return "", false
}

// normalizeSni validates and lowercases a wire SNI host_name, so it keys the reverse
// index consistently with DNS-recorded names and can't inject control bytes into the
// block-action feed or logs. Rejects empty, over-long, or non-hostname bytes.
func normalizeSni(name []byte) (string, bool) {
	if len(name) == 0 || 253 < len(name) {
		return "", false
	}
	for _, c := range name {
		switch {
		case 'a' <= c && c <= 'z':
		case 'A' <= c && c <= 'Z':
		case '0' <= c && c <= '9':
		case c == '.' || c == '-' || c == '_':
		default:
			return "", false
		}
	}
	return strings.ToLower(string(name)), true
}

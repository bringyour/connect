package connect

// this interface is a configurable blocker for both hostnames (dns) and ips (routing)
// it is meant to be able to turn on and off and internally tune dynamically by the user
// it pulls community data sets to serve as default best data sets in ip_blocker_block.go, similar to ip_security_block.go
// ip address subnets are stored the the same way in both
// hostnames are stored as peppered SHA256 hashes, using the pepper from ip_blocker_block,
// which is regenerated on each update
// the hostname check for a.b.c will check SHA256(a.b.c) exactly, then check SHA256(basename(a.b.c, 0)),
// which is the basename plus 0 extra subdomain fragments. Then it will check SHA256(basename(a.b.c, 1)),
// etc until it comes back to the exact hostname. In this way the number of hash generations and lookups depends
// on the number of fragments in the hostname. This also means a blocked hostname always blocks all its subdomains.

import (
	"crypto/sha256"
	"net/netip"
	"sync/atomic"
)

type Blocker interface {
	BlockHost(host string) bool
	BlockIp(ip netip.Addr) bool
	Enabled() bool
	SetEnabled(enabled bool)
}

// blockerPepperLen is the length of the generated pepper. the pepper is
// regenerated on every data set update, after the community lists are
// fetched, which prevents crafting upstream list entries that collide with a
// legitimate hostname in the truncated hash space (and rotates any accidental
// collision). it is not secrecy: the hash form keeps the plain text list out
// of the source and binary, but membership is testable by anyone.
const blockerPepperLen = 32

// blockerMaxHostLen is the maximum hostname length (rfc 1035)
const blockerMaxHostLen = 253

// blockerHostRecordLen is the byte length of one hostname hash record: the
// leading bytes, big endian, of SHA256(pepper || hostname). it is a size /
// collision tradeoff, and must match the generator's hostRecordLen: at 6 bytes
// a 160k-entry table costs ~0.9 MiB of read-only data, and a random hostname
// collides with an entry with p ≈ 160e3/2^48 ≈ 6e-10 per probe (a few probes
// per lookup). the pepper is regenerated on every data set update, so even that
// vanishing chance is transient rather than a permanently mis-blocked domain.
const blockerHostRecordLen = 6

// dataBlocker is the default `Blocker` over packed, read-only data tables
// (the generated community data sets in ip_blocker_block.go). the hostname
// table is searched with peppered hashes assembled in a stack buffer, and the
// ip tables use the same packed range format and searches (ip_util.go) as the
// security tables, so lookups allocate nothing on the ascii hot path.
type dataBlocker struct {
	enabled atomic.Bool

	pepper string

	hostData  string
	hostCount int

	prefixData  string
	prefixCount int

	prefix6Data  string
	prefix6Count int
}

// NewBlockerWithDefaults returns a `Blocker` over the generated default
// community data sets (ip_blocker_block.go, regenerate with blocker/main.go).
// the blocker starts disabled; the owner seeds the initial state.
func NewBlockerWithDefaults() Blocker {
	return newBlockerWithData(
		blockerPepper,
		blockerBlockedHostData, blockerBlockedHostCount,
		blockerBlockedPrefixData, blockerBlockedPrefixCount,
		blockerBlockedPrefix6Data, blockerBlockedPrefix6Count,
	)
}

func newBlockerWithData(
	pepper string,
	hostData string, hostCount int,
	prefixData string, prefixCount int,
	prefix6Data string, prefix6Count int,
) *dataBlocker {
	if len(pepper) != blockerPepperLen {
		panic("blocker pepper must be exactly blockerPepperLen bytes")
	}
	if len(hostData) != hostCount*blockerHostRecordLen {
		panic("blocker host data length does not match the record count")
	}
	if len(prefixData) != prefixCount*8 {
		panic("blocker ipv4 data length does not match the record count")
	}
	if len(prefix6Data) != prefix6Count*32 {
		panic("blocker ipv6 data length does not match the record count")
	}
	return &dataBlocker{
		pepper:       pepper,
		hostData:     hostData,
		hostCount:    hostCount,
		prefixData:   prefixData,
		prefixCount:  prefixCount,
		prefix6Data:  prefix6Data,
		prefix6Count: prefix6Count,
	}
}

func (self *dataBlocker) Enabled() bool {
	return self.enabled.Load()
}

func (self *dataBlocker) SetEnabled(enabled bool) {
	self.enabled.Store(enabled)
}

// BlockHost reports whether the hostname or any of its parent domains is in
// the blocked set (a blocked hostname always blocks all its subdomains).
// the input is normalized the same way as the data set: one trailing dot
// stripped, ascii lower cased, unicode mapped to punycode. malformed inputs
// and ip literals return false. ascii lookups allocate nothing.
func (self *dataBlocker) BlockHost(host string) bool {
	if !self.enabled.Load() {
		return false
	}
	if self.hostCount == 0 {
		return false
	}

	n := len(host)
	if 0 < n && host[n-1] == '.' {
		host = host[:n-1]
		n -= 1
	}
	if n == 0 || blockerMaxHostLen < n || host[n-1] == '.' {
		return false
	}

	// normalize into a stack buffer: lower case ascii, reject bytes that can
	// never appear in a hostname, and validate the dot structure
	var nameBuf [blockerMaxHostLen]byte
	ascii := true
	digitsDotsOnly := true
	for i := 0; i < n; i += 1 {
		c := host[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c < '!' || c == 0x7f || c == ':' {
			// control, space, delete, or a port/ipv6 separator
			return false
		}
		if c == '.' && (i == 0 || host[i-1] == '.') {
			// leading dot or empty label
			return false
		}
		if c != '.' && (c < '0' || '9' < c) {
			digitsDotsOnly = false
		}
		if 0x80 <= c {
			ascii = false
		}
		nameBuf[i] = c
	}
	if digitsDotsOnly {
		// an ipv4 literal is not a hostname (ips are matched by BlockIp)
		return false
	}
	if !ascii {
		// unicode hostname: match the data set's punycode form (allocates; rare)
		punycode, err := Punycode(string(nameBuf[:n]))
		if err != nil || len(punycode) == 0 || blockerMaxHostLen < len(punycode) {
			return false
		}
		n = copy(nameBuf[:], punycode)
	}
	name := nameBuf[:n]

	var hashBuf [blockerPepperLen + blockerMaxHostLen]byte
	copy(hashBuf[:blockerPepperLen], self.pepper)

	// the exact hostname first
	if self.blockHash(&hashBuf, name) {
		return true
	}
	// then the basename (the two label suffix) plus 0, 1, ... extra subdomain
	// fragments, until it comes back to the exact hostname. one label
	// suffixes are not checked: the generator rejects public suffix entries.
	var dots [blockerMaxHostLen/2 + 1]int
	nd := 0
	for i := 0; i < n; i += 1 {
		if name[i] == '.' {
			dots[nd] = i
			nd += 1
		}
	}
	for j := nd - 2; 0 <= j; j -= 1 {
		if self.blockHash(&hashBuf, name[dots[j]+1:]) {
			return true
		}
	}
	return false
}

// blockHash tests one candidate name against the hash table. hashBuf holds
// the pepper in its first blockerPepperLen bytes; the candidate is copied
// after it so the digest input is contiguous and stack resident.
func (self *dataBlocker) blockHash(hashBuf *[blockerPepperLen + blockerMaxHostLen]byte, name []byte) bool {
	m := copy(hashBuf[blockerPepperLen:], name)
	sum := sha256.Sum256(hashBuf[:blockerPepperLen+m])
	// the leading blockerHostRecordLen digest bytes, big endian — the same
	// construction the generator packs (blocker/main.go hashHost)
	var key uint64
	for i := 0; i < blockerHostRecordLen; i += 1 {
		key = key<<8 | uint64(sum[i])
	}
	return searchHash(self.hostData, self.hostCount, blockerHostRecordLen, key)
}

// BlockIp reports whether the address falls in a blocked range. the tables
// use the same packed range format and searches (ip_util.go) as the security
// tables.
func (self *dataBlocker) BlockIp(ip netip.Addr) bool {
	if !self.enabled.Load() {
		return false
	}
	ip = ip.Unmap()
	if ip.Is4() {
		if self.prefixCount == 0 {
			return false
		}
		b := ip.As4()
		u := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
		return searchRange4(self.prefixData, self.prefixCount, u)
	}
	if ip.Is6() {
		if self.prefix6Count == 0 {
			return false
		}
		b := ip.As16()
		return searchRange6(
			self.prefix6Data,
			self.prefix6Count,
			uint64(b[0])<<56|uint64(b[1])<<48|uint64(b[2])<<40|uint64(b[3])<<32|
				uint64(b[4])<<24|uint64(b[5])<<16|uint64(b[6])<<8|uint64(b[7]),
			uint64(b[8])<<56|uint64(b[9])<<48|uint64(b[10])<<40|uint64(b[11])<<32|
				uint64(b[12])<<24|uint64(b[13])<<16|uint64(b[14])<<8|uint64(b[15]),
		)
	}
	// the zero Addr
	return false
}

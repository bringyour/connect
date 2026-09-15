package connect

// Shared zero-allocation searches over packed, read-only blocklist tables.
// The generated tables (ip_security_cfaa_block.go, ip_blocker_block.go) are
// string constants living in the binary's read-only data; every search here
// reads them in place and allocates nothing.
//
// Formats:
//   - IPv4 ranges: 8 bytes per record — big-endian uint32 lo, then hi —
//     sorted ascending by lo and pairwise-disjoint.
//   - IPv6 ranges: 32 bytes per record — 16-byte big-endian lo, then hi —
//     same ordering.
//   - Hash records: `width` bytes per record — a big-endian unsigned integer,
//     the leading bytes of a digest — sorted ascending and unique. The width is
//     a size/collision tradeoff fixed by the generator (see
//     blockerHostRecordLen).

// be64 reads a big-endian uint64 from s at offset o, allocation-free (string
// indexing returns a byte without copying).
func be64(s string, o int) uint64 {
	return uint64(s[o])<<56 | uint64(s[o+1])<<48 | uint64(s[o+2])<<40 | uint64(s[o+3])<<32 |
		uint64(s[o+4])<<24 | uint64(s[o+5])<<16 | uint64(s[o+6])<<8 | uint64(s[o+7])
}

// searchRange4 binary-searches packed IPv4 ranges in data for ip — a
// network-order (big-endian) uint32, e.g. binary.BigEndian.Uint32(addr.To4()).
func searchRange4(data string, count int, ip uint32) bool {
	// Find the first record whose lo is greater than ip; the only range that
	// can contain ip is its predecessor (ranges are sorted and disjoint).
	lo, hi := 0, count
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		o := mid << 3
		rlo := uint32(data[o])<<24 | uint32(data[o+1])<<16 |
			uint32(data[o+2])<<8 | uint32(data[o+3])
		if rlo <= ip {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return false
	}
	o := (lo - 1) << 3
	rhi := uint32(data[o+4])<<24 | uint32(data[o+5])<<16 |
		uint32(data[o+6])<<8 | uint32(data[o+7])
	return ip <= rhi
}

// searchRange6 binary-searches packed IPv6 ranges in data for the 128-bit
// address (ipHi, ipLo) — the two-word analog of searchRange4.
func searchRange6(data string, count int, ipHi uint64, ipLo uint64) bool {
	lo, hi := 0, count
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		o := mid << 5
		rloHi := be64(data, o)
		rloLo := be64(data, o+8)
		if rloHi < ipHi || (rloHi == ipHi && rloLo <= ipLo) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return false
	}
	o := (lo - 1) << 5
	rhiHi := be64(data, o+16)
	rhiLo := be64(data, o+24)
	return ipHi < rhiHi || (ipHi == rhiHi && ipLo <= rhiLo)
}

// beN reads a big-endian unsigned integer of width bytes (width <= 8) from s at
// offset o, allocation-free.
func beN(s string, o int, width int) uint64 {
	var v uint64
	for i := 0; i < width; i += 1 {
		v = v<<8 | uint64(s[o+i])
	}
	return v
}

// searchHash binary-searches packed, sorted, unique big-endian hash records of
// width bytes each in data for an exact key match. It allocates nothing.
func searchHash(data string, count int, width int, key uint64) bool {
	lo, hi := 0, count
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if beN(data, mid*width, width) < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < count && beN(data, lo*width, width) == key
}

package pathsource

import (
	"encoding/binary"
	"net"
	"net/netip"

	"bringyour.com/connect"
	"github.com/cespare/xxhash/v2"
)

// PathToSourceAddress generates a new source address and port based on the given source address and port
// and the transfer path.
// The new source address and port are deterministic and unique for the given transfer path.
// Chances of collision are very low (2^-36).
func PathToSourceAddress(tp *connect.TransferPath, sourceAddress net.IP, sourcePort uint16) netip.AddrPort {
	xxhash := xxhash.New()
	xxhash.Write(tp.SourceId[:])
	xxhash.Write(tp.DestinationId[:])
	xxhash.Write(tp.StreamId[:])
	xxhash.Write([]byte(sourceAddress))
	xxhash.Write(binary.BigEndian.AppendUint16(nil, sourcePort))

	hash := xxhash.Sum(nil)

	firstByte := byte(hash[0])

	// make sure the first byte is not in the multicast range
	if firstByte&0xe0 == 0xe0 {
		firstByte = ^byte(0x10)
	}

	sourceIP := netip.AddrFrom4([4]byte{firstByte, hash[1], hash[2], hash[3]})

	sourcePort = binary.BigEndian.Uint16(hash[4:])

	return netip.AddrPortFrom(sourceIP, sourcePort)
}

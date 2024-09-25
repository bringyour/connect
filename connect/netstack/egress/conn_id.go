package egress

import (
	"fmt"
	"net/netip"
)

type ConnID struct {
	SourceIP   netip.Addr
	SourcePort uint16
	DestIP     netip.Addr
	DestPort   uint16
}

func (c ConnID) String() string {
	return fmt.Sprintf("%s:%d -> %s:%d", c.SourceIP, c.SourcePort, c.DestIP, c.DestPort)
}

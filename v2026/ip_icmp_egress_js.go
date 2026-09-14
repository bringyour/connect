//go:build js

package connect

import (
	"context"
	"net"
)

// no echo backend exists in the browser sandbox; flows fail soft (drop),
// exactly the pre-icmp behavior.

func newIcmpEgress(ctx context.Context, ipVersion int, destinationIp net.IP, settings *IcmpBufferSettings) (icmpEgress, error) {
	return nil, icmpEgressUnsupportedError(ipVersion)
}

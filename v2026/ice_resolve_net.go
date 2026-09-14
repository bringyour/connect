//go:build !js

// Native WebRTC address resolution must follow the same egress and lifecycle
// rules as the sockets ICE creates. Pion transport.Net exposes only the
// context-free net.Resolve* shape, so its candidate gatherer otherwise cannot
// interrupt a DNS query when a peer generation is retired.
package connect

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/pion/transport/v4"
)

// peerConnectionResolveNet gives one peer generation an independently
// cancelable resolver while preserving its underlying socket network.
type peerConnectionResolveNet struct {
	transport.Net
	ctx      context.Context
	resolver *net.Resolver
	timeout  time.Duration
}

// newPeerConnectionResolveNet creates the per-generation cancellation owner.
// The caller must cancel it before asking Pion to join candidate gathering.
func newPeerConnectionResolveNet(
	base transport.Net,
	resolver *net.Resolver,
	timeout time.Duration,
) (*peerConnectionResolveNet, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	if resolver != nil && resolver.Dial != nil {
		// net.Resolver may detach a shared wire lookup from its caller's
		// cancellation. Bind each underlying control dial to this exact peer as
		// well, so the detached lookup cannot outlive the retired generation.
		baseDial := resolver.Dial
		resolver = &net.Resolver{
			PreferGo:     resolver.PreferGo,
			StrictErrors: resolver.StrictErrors,
		}
		resolver.Dial = func(dialCtx context.Context, network string, address string) (net.Conn, error) {
			operationCtx, cancelOperation := context.WithCancelCause(dialCtx)
			stop := context.AfterFunc(ctx, func() {
				cancelOperation(context.Cause(ctx))
			})
			if ctx.Err() != nil {
				cancelOperation(context.Cause(ctx))
			}
			defer stop()
			defer cancelOperation(context.Canceled)
			return baseDial(operationCtx, network, address)
		}
	}
	return &peerConnectionResolveNet{
		Net:      base,
		ctx:      ctx,
		resolver: resolver,
		timeout:  timeout,
	}, cancel
}

// operationContext applies both the peer lifetime and the configured STUN
// gathering bound to name resolution, which Pion's timeout does not cover.
func (self *peerConnectionResolveNet) operationContext() (
	context.Context,
	context.CancelFunc,
) {
	if 0 < self.timeout {
		return context.WithTimeout(self.ctx, self.timeout)
	}
	return context.WithCancel(self.ctx)
}

// resolverOrDefault retains ordinary platform DNS when no egress resolver is
// needed on this platform.
func (self *peerConnectionResolveNet) resolverOrDefault() *net.Resolver {
	if self.resolver != nil {
		return self.resolver
	}
	return net.DefaultResolver
}

// ipNetwork maps a transport endpoint network onto net.Resolver's accepted
// LookupNetIP network names.
func ipNetwork(network string) (string, error) {
	switch network {
	case "ip", "udp", "tcp":
		return "ip", nil
	case "ip4", "udp4", "tcp4":
		return "ip4", nil
	case "ip6", "udp6", "tcp6":
		return "ip6", nil
	default:
		return "", net.UnknownNetworkError(network)
	}
}

// resolveIPAddr resolves one hostname with cancellation and preserves literal
// IPv6 zones without involving DNS.
func (self *peerConnectionResolveNet) resolveIPAddr(
	ctx context.Context,
	network string,
	host string,
) (*net.IPAddr, error) {
	lookupNetwork, err := ipNetwork(network)
	if err != nil {
		return nil, err
	}
	if host == "" {
		return &net.IPAddr{}, nil
	}
	if address, parseErr := netip.ParseAddr(host); parseErr == nil {
		return &net.IPAddr{
			IP:   net.IP(address.AsSlice()),
			Zone: address.Zone(),
		}, nil
	}
	addresses, err := self.resolverOrDefault().LookupNetIP(
		ctx,
		lookupNetwork,
		host,
	)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("resolve %s: no addresses", host)
	}
	address := addresses[0]
	return &net.IPAddr{
		IP:   net.IP(address.AsSlice()),
		Zone: address.Zone(),
	}, nil
}

// resolvePort resolves a numeric or named service within the same cancellation
// boundary as the hostname.
func (self *peerConnectionResolveNet) resolvePort(
	ctx context.Context,
	network string,
	port string,
) (int, error) {
	if number, err := strconv.Atoi(port); err == nil {
		if number < 0 || 65535 < number {
			return 0, fmt.Errorf("invalid port %q", port)
		}
		return number, nil
	}
	return self.resolverOrDefault().LookupPort(ctx, network, port)
}

// ResolveIPAddr is the cancellation-aware form used by ICE IP endpoints.
func (self *peerConnectionResolveNet) ResolveIPAddr(
	network string,
	address string,
) (*net.IPAddr, error) {
	ctx, cancel := self.operationContext()
	defer cancel()
	return self.resolveIPAddr(ctx, network, address)
}

// ResolveUDPAddr is the cancellation-aware form used by STUN and UDP TURN
// gathering.
func (self *peerConnectionResolveNet) ResolveUDPAddr(
	network string,
	address string,
) (*net.UDPAddr, error) {
	host, portValue, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ctx, cancel := self.operationContext()
	defer cancel()
	ipAddress, err := self.resolveIPAddr(ctx, network, host)
	if err != nil {
		return nil, err
	}
	port, err := self.resolvePort(ctx, "udp", portValue)
	if err != nil {
		return nil, err
	}
	return &net.UDPAddr{
		IP:   ipAddress.IP,
		Port: port,
		Zone: ipAddress.Zone,
	}, nil
}

// ResolveTCPAddr is the cancellation-aware adjacent path used by TCP and TLS
// TURN gathering.
func (self *peerConnectionResolveNet) ResolveTCPAddr(
	network string,
	address string,
) (*net.TCPAddr, error) {
	host, portValue, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ctx, cancel := self.operationContext()
	defer cancel()
	ipAddress, err := self.resolveIPAddr(ctx, network, host)
	if err != nil {
		return nil, err
	}
	port, err := self.resolvePort(ctx, "tcp", portValue)
	if err != nil {
		return nil, err
	}
	return &net.TCPAddr{
		IP:   ipAddress.IP,
		Port: port,
		Zone: ipAddress.Zone,
	}, nil
}

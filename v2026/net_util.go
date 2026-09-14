package connect

import (
	"context"
	"iter"
	"net"
	"net/netip"
	// "sync"
)

func IpsInNet(ipNet *net.IPNet) iter.Seq[net.IP] {
	return func(yield func(net.IP) bool) {
		ip := ipNet.IP.Mask(ipNet.Mask)
		ones, bits := ipNet.Mask.Size()
		n := 0x01 << (bits - ones)

		// big endian counting from lowest bit
		for k := 0; k < n; k += 1 {
			j := len(ip) - 1
			ip[j] += 1
			// propagate the overflow bit up
			for ip[j] == 0 && 0 <= j-1 {
				j -= 1
				ip[j] += 1
			}

			ipCopy := make(net.IP, len(ip))
			copy(ipCopy, ip)

			if !yield(ipCopy) {
				return
			}
		}
	}
}

func AddrsInPrefix(prefix netip.Prefix) iter.Seq[netip.Addr] {
	return func(yield func(netip.Addr) bool) {
		a := prefix.Masked().Addr()
		ip := a.AsSlice()
		n := 0x01 << (a.BitLen() - prefix.Bits())

		// big endian counting from lowest bit
		for k := 0; k < n; k += 1 {
			j := len(ip) - 1
			ip[j] += 1
			// propagate the overflow bit up
			for ip[j] == 0 && 0 <= j-1 {
				j -= 1
				ip[j] += 1
			}

			addr, _ := netip.AddrFromSlice(ip)
			if !yield(addr) {
				return
			}
		}
	}
}

// this is safe to share across goroutines
type AddrGenerator struct {
	ctx    context.Context
	cancel context.CancelFunc
	prefix netip.Prefix
	next   chan netip.Addr
	done   chan struct{}
}

func NewAddrGenerator(prefix netip.Prefix) *AddrGenerator {
	ctx, cancel := context.WithCancel(context.Background())
	ag := &AddrGenerator{
		ctx:    ctx,
		cancel: cancel,
		prefix: prefix,
		next:   make(chan netip.Addr),
		done:   make(chan struct{}),
	}
	go HandleError(ag.run)
	return ag
}

func (self *AddrGenerator) run() {
	defer self.cancel()
	defer close(self.next)
	defer close(self.done)
	for addr := range AddrsInPrefix(self.prefix) {
		select {
		case self.next <- addr:
		case <-self.ctx.Done():
			return
		}
	}
}

func (self *AddrGenerator) Next() (netip.Addr, bool) {
	addr, ok := <-self.next
	return addr, ok
}

func (self *AddrGenerator) Close() {
	self.cancel()
	<-self.done
}

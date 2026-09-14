//go:build !js

package connect

import (
	"net"
	"syscall"

	"github.com/pion/transport/v4"
	"github.com/pion/transport/v4/stdnet"
)

// egressNet is a pion transport.Net that binds the UDP sockets pion creates for
// ICE to the egress interface, so p2p (webrtc) traffic does not loop back into
// the tunnel this process provides (R1). It embeds the standard net and only
// overrides socket creation; a no-op unless an egress index is set / off
// Windows.
type egressNet struct {
	transport.Net
	log Logger
}

func newEgressNet(log Logger) (transport.Net, error) {
	base, err := stdnet.NewNet()
	if err != nil {
		return nil, err
	}
	return &egressNet{Net: base, log: loggerOrDefault(log)}, nil
}

// A socket we could not pin still works -- it just works through the wrong
// interface, which for an ICE candidate means offering the tun's address to a
// peer and gathering a path that loops back into this process. Not worth
// failing the peer connection over, very much worth saying.
func (self *egressNet) logBindFailure(err error) {
	if err == nil {
		return
	}
	self.log.Infof("[egress]ice socket not pinned to the physical interface, p2p may loop into the tunnel: %s\n", err)
}

func (self *egressNet) ListenUDP(network string, locAddr *net.UDPAddr) (transport.UDPConn, error) {
	conn, err := self.Net.ListenUDP(network, locAddr)
	if err != nil {
		return nil, err
	}
	if sc, ok := conn.(syscall.Conn); ok {
		self.logBindFailure(applyEgress(sc))
	}
	return conn, nil
}

func (self *egressNet) ListenPacket(network string, address string) (net.PacketConn, error) {
	conn, err := self.Net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	if sc, ok := conn.(syscall.Conn); ok {
		self.logBindFailure(applyEgress(sc))
	}
	return conn, nil
}

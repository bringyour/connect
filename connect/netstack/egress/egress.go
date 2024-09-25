package egress

import (
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"

	"bringyour.com/connect/netstack"
	"github.com/golang/glog"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"golang.org/x/sync/errgroup"
)

type Egress struct {
	dev              netstack.Device
	net              *netstack.Net
	runningListeners map[uint16]func()
	mu               sync.Mutex
}

func NewEgress(dev netstack.Device, net *netstack.Net) *Egress {
	e := &Egress{
		dev:              dev,
		net:              net,
		runningListeners: make(map[uint16]func()),
	}
	return e
}

func (e *Egress) Write(pkt []byte) (int, error) {

	cid, ok := syncPacketData(pkt)
	if ok {
		fmt.Println("SYN packet detected", cid)
	}

	if ok {
		e.mu.Lock()

		registerFunc, ok := e.runningListeners[cid.DestPort]
		if !ok {
			registerFunc = sync.OnceFunc(func() {
				fmt.Println("registering listener", cid.DestPort)
				defer func() {
					fmt.Println("listener registered", cid.DestPort)
				}()

				list, err := e.net.ListenTCP(&net.TCPAddr{
					IP:   net.IPv4zero,
					Port: int(cid.DestPort),
				})
				if err != nil {
					glog.Error("failed to listen", err)
				}

				go func() {
					for {
						c, err := list.Accept()
						if err != nil {
							glog.Error("failed to accept", err)
							return
						}
						go func() {
							defer c.Close()
							local := c.LocalAddr()

							addr := local.(*net.TCPAddr)
							oc, err := net.DialTCP("tcp", nil, addr)
							if err != nil {
								glog.Error("failed to dial", err)
								return
							}
							defer oc.Close()

							eg := errgroup.Group{}
							eg.Go(func() error {
								_, err := io.Copy(oc, c)
								return err
							})

							eg.Go(func() error {
								_, err := io.Copy(c, oc)
								return err
							})

							err = eg.Wait()
							if err != nil {
								glog.Error("failed to copy", err)
							}

						}()
					}
				}()
			})
			e.runningListeners[cid.DestPort] = registerFunc
		}

		e.mu.Unlock()

		registerFunc()
	}

	return e.dev.Write(pkt)

}

func (e *Egress) Read(pkt []byte) (int, error) {
	return e.dev.Read(pkt)
}

func syncPacketData(packet []byte) (ConnID, bool) {
	switch packet[0] >> 4 {
	case 4:
		pk := gopacket.NewPacket(packet, layers.LayerTypeIPv4, gopacket.NoCopy)
		v4Layer, _ := pk.Layer(layers.LayerTypeIPv4).(*layers.IPv4)
		if v4Layer == nil {
			return ConnID{}, false
		}
		tcpLayer := pk.Layer(layers.LayerTypeTCP).(*layers.TCP)
		if tcpLayer == nil {
			return ConnID{}, false
		}

		if tcpLayer.SYN && !tcpLayer.ACK {
			return ConnID{
				SourceIP:   netip.AddrFrom4([4]byte(v4Layer.SrcIP)),
				SourcePort: uint16(tcpLayer.SrcPort),
				DestIP:     netip.AddrFrom4([4]byte(v4Layer.DstIP)),
				DestPort:   uint16(tcpLayer.DstPort),
			}, true

		}

	case 6:
		pk := gopacket.NewPacket(packet, layers.LayerTypeIPv6, gopacket.NoCopy)
		v6Layer, _ := pk.Layer(layers.LayerTypeIPv6).(*layers.IPv6)
		if v6Layer == nil {
			return ConnID{}, false
		}
		tcpLayer := pk.Layer(layers.LayerTypeTCP).(*layers.TCP)
		if tcpLayer == nil {
			return ConnID{}, false
		}

		if tcpLayer.SYN && !tcpLayer.ACK {
			return ConnID{
				SourceIP:   netip.AddrFrom16([16]byte(v6Layer.SrcIP)),
				SourcePort: uint16(tcpLayer.SrcPort),
				DestIP:     netip.AddrFrom16([16]byte(v6Layer.DstIP)),
				DestPort:   uint16(tcpLayer.DstPort),
			}, true
		}

	}

	return ConnID{}, false

}

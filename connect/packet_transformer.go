package connect

import (
	"context"
	"fmt"
	"net/netip"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

type PacketTransformer struct {
	sm *SourceMapper
}

// NewPacketTransformer creates a new PacketTransformer.
func NewPacketTransformer(ctx context.Context) *PacketTransformer {

	return &PacketTransformer{
		sm: NewSourceMapper(ctx),
	}
}

func (p *PacketTransformer) RewritePacketFromVPN(buffer []byte, tp TransferPath) ([]byte, error) {

	ipVersion := uint8(buffer[0]) >> 4

	switch ipVersion {
	case 4:

		// gopacket.NewPacket(buffer, layers.LayerTypeIPv4, gopacket.Default)

		ipv4 := &layers.IPv4{}
		err := ipv4.DecodeFromBytes(buffer, gopacket.NilDecodeFeedback)
		if err != nil {
			return nil, fmt.Errorf("failed to decode IPv4 layer: %w", err)
		}

		switch ipv4.Protocol {
		case layers.IPProtocolTCP:
			tcp := &layers.TCP{}
			err := tcp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)
			if err != nil {
				return nil, fmt.Errorf("failed to decode TCP layer: %w", err)
			}

			ipv4.Checksum = 0
			tcp.SetNetworkLayerForChecksum(ipv4)
			tcp.Checksum = 0

			mapped := p.sm.GetSourceAddressMapping(netip.AddrFrom4([4]byte(ipv4.SrcIP)), tp)

			ipv4.SrcIP = mapped.AsSlice()

			rewrittenBuffer := gopacket.NewSerializeBuffer()
			options := gopacket.SerializeOptions{
				FixLengths:       true,
				ComputeChecksums: true,
			}

			err = gopacket.SerializeLayers(rewrittenBuffer, options,
				ipv4,
				tcp,
				gopacket.Payload(tcp.LayerPayload()),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize packet: %w", err)
			}

			return rewrittenBuffer.Bytes(), nil

		}

		return buffer, nil

	// case 6:
	// 	ipv6 := layers.IPv6{}
	// 	ipv6.DecodeFromBytes(buffer[0:n], gopacket.NilDecodeFeedback)
	// 	switch ipv6.NextHeader {
	// 	case layers.IPProtocolTCP:
	// 		tcp := layers.TCP{}
	// 		tcp.DecodeFromBytes(ipv6.Payload, gopacket.NilDecodeFeedback)

	// 		sourceId := SourceID{
	// 			ip:   netip.AddrFrom16([16]byte(ipv6.DstIP)),
	// 			port: uint16(tcp.DstPort),
	// 		}

	// 		sourceMapLock.Lock()
	// 		epa := sourceMap[sourceId]
	// 		sourceMapLock.Unlock()

	// 		tp = epa.transferPath

	// 	}

	default:
		return buffer, nil
	}
}

func (p *PacketTransformer) RewritePacketToVPN(buffer []byte) ([]byte, *TransferPath, error) {
	ipVersion := uint8(buffer[0]) >> 4

	switch ipVersion {
	case 4:

		// gopacket.NewPacket(buffer, layers.LayerTypeIPv4, gopacket.Default)

		ipv4 := &layers.IPv4{}
		err := ipv4.DecodeFromBytes(buffer, gopacket.NilDecodeFeedback)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode IPv4 layer: %w", err)
		}

		switch ipv4.Protocol {
		case layers.IPProtocolTCP:
			tcp := &layers.TCP{}
			err := tcp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to decode TCP layer: %w", err)
			}

			ipv4.Checksum = 0
			tcp.SetNetworkLayerForChecksum(ipv4)
			tcp.Checksum = 0

			realEndpointAddress, ok := p.sm.GetRealEndpointAddress(netip.AddrFrom4([4]byte(ipv4.DstIP)))
			if !ok {
				return nil, nil, fmt.Errorf("failed to find real address mapping for %s", netip.AddrFrom4([4]byte(ipv4.DstIP)))
			}

			ipv4.DstIP = realEndpointAddress.realSource.AsSlice()

			rewrittenBuffer := gopacket.NewSerializeBuffer()
			options := gopacket.SerializeOptions{
				FixLengths:       true,
				ComputeChecksums: true,
			}

			err = gopacket.SerializeLayers(rewrittenBuffer, options,
				ipv4,
				tcp,
				gopacket.Payload(tcp.LayerPayload()),
			)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to serialize packet: %w", err)
			}

			return rewrittenBuffer.Bytes(), &realEndpointAddress.transferPath, nil

		}

	}

	return nil, nil, fmt.Errorf("unsupported ip version: %d", ipVersion)

}

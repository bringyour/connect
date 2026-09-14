package connect

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

func ToFrame(message proto.Message, protocolVersion int) (*protocol.Frame, error) {
	var messageType protocol.MessageType
	switch v := message.(type) {
	case *protocol.Pack:
		messageType = protocol.MessageType_TransferPack
	case *protocol.Ack:
		messageType = protocol.MessageType_TransferAck
	case *protocol.Contract:
		messageType = protocol.MessageType_TransferContract
	case *protocol.Provide:
		messageType = protocol.MessageType_TransferProvide
	case *protocol.Auth:
		messageType = protocol.MessageType_TransferAuth
	case *protocol.StreamOpen:
		messageType = protocol.MessageType_TransferStreamOpen
	case *protocol.StreamClose:
		messageType = protocol.MessageType_TransferStreamClose
	case *protocol.CreateContract:
		messageType = protocol.MessageType_TransferCreateContract
	case *protocol.CreateContractResult:
		messageType = protocol.MessageType_TransferCreateContractResult
	case *protocol.CloseContract:
		messageType = protocol.MessageType_TransferCloseContract
	case *protocol.PeerAudit:
		messageType = protocol.MessageType_TransferPeerAudit
	case *protocol.SimpleMessage:
		messageType = protocol.MessageType_TestSimpleMessage
	case *protocol.IpPacketToProvider:
		messageType = protocol.MessageType_IpIpPacketToProvider
		if 2 <= protocolVersion {
			return &protocol.Frame{
				MessageType:  messageType,
				MessageBytes: v.IpPacket.PacketBytes,
				Raw:          true,
			}, nil
		}
	case *protocol.IpPacketFromProvider:
		messageType = protocol.MessageType_IpIpPacketFromProvider
		if 2 <= protocolVersion {
			return &protocol.Frame{
				MessageType:  messageType,
				MessageBytes: v.IpPacket.PacketBytes,
				Raw:          true,
			}, nil
		}
	case *protocol.IpPing:
		messageType = protocol.MessageType_IpIpPing
		if 2 <= protocolVersion {
			return &protocol.Frame{
				MessageType: messageType,
				Raw:         true,
			}, nil
		}
	case *protocol.IpProviderDiagnostics:
		messageType = protocol.MessageType_IpIpProviderDiagnostics
	case *protocol.ControlPing:
		messageType = protocol.MessageType_TransferControlPing
	case *protocol.ProvidePing:
		messageType = protocol.MessageType_TransferProvidePing
	case *protocol.ExchangeSignals:
		messageType = protocol.MessageType_TransferExchangeSignals
	case *protocol.ExchangeSignal:
		messageType = protocol.MessageType_TransferExchangeSignal
	case *protocol.StreamReset:
		messageType = protocol.MessageType_TransferStreamReset
	case *protocol.EncryptedKey:
		messageType = protocol.MessageType_TransferEncryptedKey
	case *protocol.ClientKey:
		messageType = protocol.MessageType_TransferClientKey
	case *protocol.NetworkPeersReset:
		messageType = protocol.MessageType_TransferNetworkPeersReset
	case *protocol.NetworkPeersUpdate:
		messageType = protocol.MessageType_TransferNetworkPeersUpdate
	case *protocol.ResidentMigrate:
		messageType = protocol.MessageType_TransferResidentMigrate
	case *protocol.SubprotocolMessage:
		// the generic path; the send functions in subprotocol.go hand-roll
		// this encoding so the codec writes the payload in place
		messageType = protocol.MessageType_Subprotocol
	case *protocol.SubprotocolsQuery:
		messageType = protocol.MessageType_TransferSubprotocolsQuery
	case *protocol.SubprotocolsQueryResult:
		messageType = protocol.MessageType_TransferSubprotocolsQueryResult
	default:
		return nil, fmt.Errorf("Unknown message type: %T", v)
	}
	b, err := ProtoMarshal(message)
	if err != nil {
		return nil, err
	}
	return &protocol.Frame{
		MessageType:  messageType,
		MessageBytes: b,
	}, nil
}

// ipPacketToProviderFrame builds the egress send frame for a raw IP packet.
// on the v2+ raw path (the default) the packet bytes are carried directly on
// the Frame, so this avoids allocating the protocol.IpPacketToProvider and
// protocol.IpPacket wrappers that ToFrame would build and immediately discard.
// the legacy (<v2) path falls back to the wrapped encoding.
func ipPacketToProviderFrame(packet []byte, protocolVersion int) (*protocol.Frame, error) {
	if 2 <= protocolVersion {
		return &protocol.Frame{
			MessageType:  protocol.MessageType_IpIpPacketToProvider,
			MessageBytes: packet,
			Raw:          true,
		}, nil
	}
	return ToFrame(&protocol.IpPacketToProvider{
		IpPacket: &protocol.IpPacket{
			PacketBytes: packet,
		},
	}, protocolVersion)
}

// ipPacketFromProviderFrame mirrors ipPacketToProviderFrame for the provider's
// return path, avoiding the wrapper allocations on the v2+ raw path.
func ipPacketFromProviderFrame(packet []byte, protocolVersion int) (*protocol.Frame, error) {
	if 2 <= protocolVersion {
		return &protocol.Frame{
			MessageType:  protocol.MessageType_IpIpPacketFromProvider,
			MessageBytes: packet,
			Raw:          true,
		}, nil
	}
	return ToFrame(&protocol.IpPacketFromProvider{
		IpPacket: &protocol.IpPacket{
			PacketBytes: packet,
		},
	}, protocolVersion)
}

// ipPacketToProviderBytes returns the packet carried by an IP frame without
// materializing the two legacy protobuf wrapper objects on the v2+ raw hot
// path. The returned bytes have the same callback-scoped lifetime as frame.
func ipPacketToProviderBytes(frame *protocol.Frame) ([]byte, error) {
	if frame.Raw {
		return frame.MessageBytes, nil
	}
	message, err := FromFrame(frame)
	if err != nil {
		return nil, err
	}
	return message.(*protocol.IpPacketToProvider).IpPacket.PacketBytes, nil
}

// ipPacketFromProviderBytes is the return-path counterpart to
// ipPacketToProviderBytes.
func ipPacketFromProviderBytes(frame *protocol.Frame) ([]byte, error) {
	if frame.Raw {
		return frame.MessageBytes, nil
	}
	message, err := FromFrame(frame)
	if err != nil {
		return nil, err
	}
	return message.(*protocol.IpPacketFromProvider).IpPacket.PacketBytes, nil
}

func RequireToFrameWithDefaultProtocolVersion(message proto.Message) *protocol.Frame {
	return RequireToFrame(message, DefaultProtocolVersion)
}

func RequireToFrame(message proto.Message, protocolVersion int) *protocol.Frame {
	frame, err := ToFrame(message, protocolVersion)
	if err != nil {
		panic(err)
	}
	return frame
}

func FromFrame(frame *protocol.Frame) (proto.Message, error) {
	var message proto.Message
	switch frame.MessageType {
	case protocol.MessageType_TransferPack:
		message = &protocol.Pack{}
	case protocol.MessageType_TransferAck:
		message = &protocol.Ack{}
	case protocol.MessageType_TransferContract:
		message = &protocol.Contract{}
	case protocol.MessageType_TransferProvide:
		message = &protocol.Provide{}
	case protocol.MessageType_TransferAuth:
		message = &protocol.Auth{}
	case protocol.MessageType_TransferStreamOpen:
		message = &protocol.StreamOpen{}
	case protocol.MessageType_TransferStreamClose:
		message = &protocol.StreamClose{}
	case protocol.MessageType_TransferCreateContract:
		message = &protocol.CreateContract{}
	case protocol.MessageType_TransferCreateContractResult:
		message = &protocol.CreateContractResult{}
	case protocol.MessageType_TransferCloseContract:
		message = &protocol.CloseContract{}
	case protocol.MessageType_TransferPeerAudit:
		message = &protocol.PeerAudit{}
	case protocol.MessageType_TestSimpleMessage:
		message = &protocol.SimpleMessage{}
	case protocol.MessageType_IpIpPacketToProvider:
		v := &protocol.IpPacketToProvider{}
		if frame.Raw {
			v.IpPacket = &protocol.IpPacket{
				PacketBytes: frame.MessageBytes,
			}
			return v, nil
		}
		message = v
	case protocol.MessageType_IpIpPacketFromProvider:
		v := &protocol.IpPacketFromProvider{}
		if frame.Raw {
			v.IpPacket = &protocol.IpPacket{
				PacketBytes: frame.MessageBytes,
			}
			return v, nil
		}
		message = v
	case protocol.MessageType_IpIpPing:
		v := &protocol.IpPing{}
		if frame.Raw {
			return v, nil
		}
		message = v
	case protocol.MessageType_IpIpProviderDiagnostics:
		message = &protocol.IpProviderDiagnostics{}
	case protocol.MessageType_TransferControlPing:
		message = &protocol.ControlPing{}
	case protocol.MessageType_TransferProvidePing:
		message = &protocol.ProvidePing{}
	case protocol.MessageType_TransferExchangeSignals:
		message = &protocol.ExchangeSignals{}
	case protocol.MessageType_TransferExchangeSignal:
		message = &protocol.ExchangeSignal{}
	case protocol.MessageType_TransferStreamReset:
		message = &protocol.StreamReset{}
	case protocol.MessageType_TransferEncryptedKey:
		message = &protocol.EncryptedKey{}
	case protocol.MessageType_TransferClientKey:
		message = &protocol.ClientKey{}
	case protocol.MessageType_TransferNetworkPeersReset:
		message = &protocol.NetworkPeersReset{}
	case protocol.MessageType_TransferNetworkPeersUpdate:
		message = &protocol.NetworkPeersUpdate{}
	case protocol.MessageType_TransferResidentMigrate:
		message = &protocol.ResidentMigrate{}
	case protocol.MessageType_Subprotocol:
		// the generic path copies the payload; the client's dispatch decodes
		// the header in place instead (subprotocol.go)
		message = &protocol.SubprotocolMessage{}
	case protocol.MessageType_TransferSubprotocolsQuery:
		message = &protocol.SubprotocolsQuery{}
	case protocol.MessageType_TransferSubprotocolsQueryResult:
		message = &protocol.SubprotocolsQueryResult{}
	default:
		return nil, fmt.Errorf("Unknown message type: %s", frame.MessageType)
	}
	err := ProtoUnmarshal(frame.GetMessageBytes(), message)
	if err != nil {
		return nil, err
	}
	return message, nil
}

// func RequireFromFrame(frame *protocol.Frame) proto.Message {
// 	message, err := FromFrame(frame)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return message
// }

func EncodeFrame(message proto.Message, protocolVersion int) ([]byte, error) {
	frame, err := ToFrame(message, protocolVersion)
	if err != nil {
		return nil, err
	}
	b, err := ProtoMarshal(frame)
	if !frame.Raw {
		MessagePoolReturn(frame.MessageBytes)
	}
	return b, err
}

func DecodeFrame(b []byte) (proto.Message, error) {
	frame := &protocol.Frame{}
	err := ProtoUnmarshal(b, frame)
	if err != nil {
		return nil, err
	}
	return FromFrame(frame)
}

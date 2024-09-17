package connect

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"bringyour.com/protocol"
)

func ToFrame(message proto.Message) (*protocol.Frame, error) {
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
	case *protocol.IpPacketFromProvider:
		messageType = protocol.MessageType_IpIpPacketFromProvider
	case *protocol.IpPing:
		messageType = protocol.MessageType_IpIpPing
	case *protocol.ControlPing:
		messageType = protocol.MessageType_TransferControlPing
	case *protocol.ProvidePing:
		messageType = protocol.MessageType_TransferProvidePing
	default:
		return nil, fmt.Errorf("Unknown message type: %T", v)
	}
	b, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	return &protocol.Frame{
		MessageType:  messageType,
		MessageBytes: b,
	}, nil
}

func RequireToFrame(message proto.Message) *protocol.Frame {
	frame, err := ToFrame(message)
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
		message = &protocol.IpPacketToProvider{}
	case protocol.MessageType_IpIpPacketFromProvider:
		message = &protocol.IpPacketFromProvider{}
	case protocol.MessageType_IpIpPing:
		message = &protocol.IpPing{}
	case protocol.MessageType_TransferControlPing:
		message = &protocol.ControlPing{}
	case protocol.MessageType_TransferProvidePing:
		message = &protocol.ProvidePing{}
	default:
		return nil, fmt.Errorf("Unknown message type: %s", frame.MessageType)
	}
	err := proto.Unmarshal(frame.GetMessageBytes(), message)
	if err != nil {
		return nil, err
	}
	return message, nil
}

func RequireFromFrame(frame *protocol.Frame) proto.Message {
	message, err := FromFrame(frame)
	if err != nil {
		panic(err)
	}
	return message
}

func EncodeFrame(message proto.Message) ([]byte, error) {
	frame, err := ToFrame(message)
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(frame)
	return b, err
}

func DecodeFrame(b []byte) (proto.Message, error) {
	frame := &protocol.Frame{}
	err := proto.Unmarshal(b, frame)
	if err != nil {
		return nil, err
	}
	return FromFrame(frame)
}

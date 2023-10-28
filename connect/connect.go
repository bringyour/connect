package connect

import (
	"errors"
	// "log"
	"fmt"
	"encoding/hex"

	"google.golang.org/protobuf/proto"

	"github.com/oklog/ulid/v2"

	"bringyour.com/protocol"
)


type Id [16]byte

func NewId() Id {
	return Id(ulid.Make())
}

func IdFromBytes(idBytes []byte) (Id, error) {
	if len(idBytes) != 16 {
		return Id{}, errors.New("Id must be 16 bytes")
	}
	return Id(idBytes), nil
}

func RequireIdFromBytes(idBytes []byte) Id {
	id, err := IdFromBytes(idBytes)
	if err != nil {
		panic(err)
	}
	return id
}

func ParseId(idStr string) (Id, error) {
	return parseUuid(idStr) 
}

func (self *Id) Bytes() []byte {
	return self[0:16]
}

func (self *Id) String() string {
	return encodeUuid(*self)
}


func parseUuid(src string) (dst [16]byte, err error) {
	switch len(src) {
	case 36:
		src = src[0:8] + src[9:13] + src[14:18] + src[19:23] + src[24:]
	case 32:
		// dashes already stripped, assume valid
	default:
		// assume invalid.
		return dst, fmt.Errorf("cannot parse UUID %v", src)
	}

	buf, err := hex.DecodeString(src)
	if err != nil {
		return dst, err
	}

	copy(dst[:], buf)
	return dst, err
}


func encodeUuid(src [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", src[0:4], src[4:6], src[6:8], src[8:10], src[10:16])
}



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
	case *protocol.CreateStream:
		messageType = protocol.MessageType_TransferCreateStream
	case *protocol.CreateStreamResult:
		messageType = protocol.MessageType_TransferCreateStreamResult
	case *protocol.CloseStream:
		messageType = protocol.MessageType_TransferCloseStream
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
	default:
		return nil, fmt.Errorf("Unknown message type: %T", v)
	}
	b, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	return &protocol.Frame{
		MessageType: messageType,
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
	case protocol.MessageType_TransferCreateStream:
		message = &protocol.CreateStream{}
	case protocol.MessageType_TransferCreateStreamResult:
		message = &protocol.CreateStreamResult{}
	case protocol.MessageType_TransferCloseStream:
		message = &protocol.CloseStream{}
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

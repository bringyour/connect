package connect

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	mathrand "math/rand"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"

	"golang.org/x/exp/maps"

	// "google.golang.org/protobuf/proto"

	"github.com/golang/glog"

	"bringyour.com/protocol"
)

// implements user-space NAT (UNAT) and packet inspection
// The UNAT emulates a raw socket using user-space sockets.

// use 0 for deadlock testing
const DefaultIpBufferSize = 32

const DefaultMtu = 1440
const Ipv4HeaderSizeWithoutExtensions = 20
const Ipv6HeaderSize = 40
const UdpHeaderSize = 8
const TcpHeaderSizeWithoutExtensions = 20

const debugVerifyHeaders = false

// send from a raw socket
// note `ipProtocol` is not supplied. The implementation must do a packet inspection to determine protocol
// `provideMode` is the relationship between the source and this device
type SendPacketFunction func(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool

// receive into a raw socket
type ReceivePacketFunction func(source TransferPath, ipProtocol IpProtocol, packet []byte)

type UserNatClient interface {
	// `SendPacketFunction`
	SendPacket(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool
	Close()
	Shuffle()
}

func DefaultUdpBufferSettings() *UdpBufferSettings {
	return &UdpBufferSettings{
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
		Mtu:          DefaultMtu,
		// avoid fragmentation
		ReadBufferByteCount: DefaultMtu - max(Ipv4HeaderSizeWithoutExtensions, Ipv6HeaderSize) - max(UdpHeaderSize, TcpHeaderSizeWithoutExtensions),
		SequenceBufferSize:  DefaultIpBufferSize,
		UserLimit:           128,
	}
}

func DefaultTcpBufferSettings() *TcpBufferSettings {
	tcpBufferSettings := &TcpBufferSettings{
		ConnectTimeout:     60 * time.Second,
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       15 * time.Second,
		AckCompressTimeout: time.Duration(0),
		IdleTimeout:        60 * time.Second,
		SequenceBufferSize: DefaultIpBufferSize,
		Mtu:                DefaultMtu,
		// avoid fragmentation
		ReadBufferByteCount: DefaultMtu - max(Ipv4HeaderSizeWithoutExtensions, Ipv6HeaderSize) - max(UdpHeaderSize, TcpHeaderSizeWithoutExtensions),
		WindowSize:          uint32(kib(128)),
		UserLimit:           128,
	}
	return tcpBufferSettings
}

func DefaultLocalUserNatSettings() *LocalUserNatSettings {
	return &LocalUserNatSettings{
		SequenceBufferSize: DefaultIpBufferSize,
		BufferTimeout:      15 * time.Second,
		UdpBufferSettings:  DefaultUdpBufferSettings(),
		TcpBufferSettings:  DefaultTcpBufferSettings(),
	}
}

type LocalUserNatSettings struct {
	SequenceBufferSize int
	BufferTimeout      time.Duration
	UdpBufferSettings  *UdpBufferSettings
	TcpBufferSettings  *TcpBufferSettings
}

// forwards packets using user space sockets
// this assumes transfer between the packet source and this is lossless and in order,
// so the protocol stack implementations do not implement any retransmit logic
type LocalUserNat struct {
	ctx       context.Context
	cancel    context.CancelFunc
	clientTag string

	sendPackets chan *SendPacket

	settings *LocalUserNatSettings

	// receive callback
	receiveCallbacks *CallbackList[ReceivePacketFunction]
}

func NewLocalUserNatWithDefaults(ctx context.Context, clientTag string) *LocalUserNat {
	return NewLocalUserNat(ctx, clientTag, DefaultLocalUserNatSettings())
}

func NewLocalUserNat(ctx context.Context, clientTag string, settings *LocalUserNatSettings) *LocalUserNat {
	cancelCtx, cancel := context.WithCancel(ctx)

	localUserNat := &LocalUserNat{
		ctx:              cancelCtx,
		cancel:           cancel,
		clientTag:        clientTag,
		sendPackets:      make(chan *SendPacket, settings.SequenceBufferSize),
		settings:         settings,
		receiveCallbacks: NewCallbackList[ReceivePacketFunction](),
	}
	go localUserNat.Run()

	return localUserNat
}

// TODO provide mode of the destination determines filtering rules - e.g. local networks
// TODO currently filter all local networks and non-encrypted traffic
func (self *LocalUserNat) SendPacketWithTimeout(source TransferPath, provideMode protocol.ProvideMode,
	packet []byte, timeout time.Duration) bool {
	sendPacket := &SendPacket{
		source:      source,
		provideMode: provideMode,
		packet:      packet,
	}
	if timeout < 0 {
		select {
		case <-self.ctx.Done():
			return false
		case self.sendPackets <- sendPacket:
			return true
		}
	} else if 0 == timeout {
		select {
		case <-self.ctx.Done():
			return false
		case self.sendPackets <- sendPacket:
			return true
		default:
			// full
			return false
		}
	} else {
		select {
		case <-self.ctx.Done():
			return false
		case self.sendPackets <- sendPacket:
			return true
		case <-time.After(timeout):
			// full
			return false
		}
	}
}

// `SendPacketFunction`
func (self *LocalUserNat) SendPacket(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
	return self.SendPacketWithTimeout(source, provideMode, packet, timeout)
}

// func (self *LocalUserNat) ReceiveN(source TransferPath, provideMode protocol.ProvideMode, packet []byte, n int) {
//     self.Receive(source, provideMode, packet[0:n])
// }

func (self *LocalUserNat) AddReceivePacketCallback(receiveCallback ReceivePacketFunction) func() {
	callbackId := self.receiveCallbacks.Add(receiveCallback)
	return func() {
		self.receiveCallbacks.Remove(callbackId)
	}
}

// func (self *LocalUserNat) RemoveReceivePacketCallback(receiveCallback ReceivePacketFunction) {
//     self.receiveCallbacks.Remove(receiveCallback)
// }

// `ReceivePacketFunction`
func (self *LocalUserNat) receive(source TransferPath, ipProtocol IpProtocol, packet []byte) {
	for _, receiveCallback := range self.receiveCallbacks.Get() {
		HandleError(func() {
			receiveCallback(source, ipProtocol, packet)
		})
	}
}

func (self *LocalUserNat) Run() {
	defer self.cancel()

	udp4Buffer := NewUdp4Buffer(self.ctx, self.receive, self.settings.UdpBufferSettings)
	udp6Buffer := NewUdp6Buffer(self.ctx, self.receive, self.settings.UdpBufferSettings)
	tcp4Buffer := NewTcp4Buffer(self.ctx, self.receive, self.settings.TcpBufferSettings)
	tcp6Buffer := NewTcp6Buffer(self.ctx, self.receive, self.settings.TcpBufferSettings)

	for {
		select {
		case <-self.ctx.Done():
			return
		case sendPacket := <-self.sendPackets:
			ipPacket := sendPacket.packet
			ipVersion := uint8(ipPacket[0]) >> 4
			switch ipVersion {
			case 4:
				ipv4 := layers.IPv4{}
				ipv4.DecodeFromBytes(ipPacket, gopacket.NilDecodeFeedback)
				switch ipv4.Protocol {
				case layers.IPProtocolUDP:
					udp := layers.UDP{}
					udp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

					c := func() bool {
						success, err := udp4Buffer.send(
							sendPacket.source,
							sendPacket.provideMode,
							&ipv4,
							&udp,
							self.settings.BufferTimeout,
						)
						return success && err == nil
					}
					if glog.V(2) {
						TraceWithReturn(
							fmt.Sprintf("[lnr]send udp4 %s<-%s s(%s)", self.clientTag, sendPacket.source.SourceId, sendPacket.source.StreamId),
							c,
						)
					} else {
						c()
					}
				case layers.IPProtocolTCP:
					tcp := layers.TCP{}
					tcp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

					c := func() bool {
						success, err := tcp4Buffer.send(
							sendPacket.source,
							sendPacket.provideMode,
							&ipv4,
							&tcp,
							self.settings.BufferTimeout,
						)
						return success && err == nil
					}
					if glog.V(2) {
						TraceWithReturn(
							fmt.Sprintf("[lnr]send tcp4 %s<-%s s(%s)", self.clientTag, sendPacket.source.SourceId, sendPacket.source.StreamId),
							c,
						)
					} else {
						c()
					}
				default:
					// no support for this protocol, drop
				}
			case 6:
				ipv6 := layers.IPv6{}
				ipv6.DecodeFromBytes(ipPacket, gopacket.NilDecodeFeedback)
				switch ipv6.NextHeader {
				case layers.IPProtocolUDP:
					udp := layers.UDP{}
					udp.DecodeFromBytes(ipv6.Payload, gopacket.NilDecodeFeedback)

					c := func() bool {
						success, err := udp6Buffer.send(
							sendPacket.source,
							sendPacket.provideMode,
							&ipv6,
							&udp,
							self.settings.BufferTimeout,
						)
						return success && err == nil
					}
					if glog.V(2) {
						TraceWithReturn(
							fmt.Sprintf("[lnr]send udp6 %s<-%s s(%s)", self.clientTag, sendPacket.source.SourceId, sendPacket.source.StreamId),
							c,
						)
					} else {
						c()
					}
				case layers.IPProtocolTCP:
					tcp := layers.TCP{}
					tcp.DecodeFromBytes(ipv6.Payload, gopacket.NilDecodeFeedback)

					c := func() bool {
						success, err := tcp6Buffer.send(
							sendPacket.source,
							sendPacket.provideMode,
							&ipv6,
							&tcp,
							self.settings.BufferTimeout,
						)
						return success && err == nil
					}
					if glog.V(2) {
						TraceWithReturn(
							fmt.Sprintf("[lnr]send tcp6 %s<-%s s(%s)", self.clientTag, sendPacket.source.SourceId, sendPacket.source.StreamId),
							c,
						)
					} else {
						c()
					}
				default:
					// no support for this protocol, drop
				}
			}
		}
	}
}

func (self *LocalUserNat) Close() {
	self.cancel()
}

type SendPacket struct {
	source      TransferPath
	provideMode protocol.ProvideMode
	packet      []byte
}

// comparable
type BufferId4 struct {
	source          TransferPath
	sourceIp        [4]byte
	sourcePort      int
	destinationIp   [4]byte
	destinationPort int
}

func NewBufferId4(source TransferPath, sourceIp net.IP, sourcePort int, destinationIp net.IP, destinationPort int) BufferId4 {
	return BufferId4{
		source:          source,
		sourceIp:        [4]byte(sourceIp),
		sourcePort:      sourcePort,
		destinationIp:   [4]byte(destinationIp),
		destinationPort: destinationPort,
	}
}

// comparable
type BufferId6 struct {
	source          TransferPath
	sourceIp        [16]byte
	sourcePort      int
	destinationIp   [16]byte
	destinationPort int
}

func NewBufferId6(source TransferPath, sourceIp net.IP, sourcePort int, destinationIp net.IP, destinationPort int) BufferId6 {
	return BufferId6{
		source:          source,
		sourceIp:        [16]byte(sourceIp),
		sourcePort:      sourcePort,
		destinationIp:   [16]byte(destinationIp),
		destinationPort: destinationPort,
	}
}

type UdpBufferSettings struct {
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	IdleTimeout         time.Duration
	Mtu                 int
	ReadBufferByteCount int
	SequenceBufferSize  int
	// the number of open sockets per user
	// uses an lru cleanup where new sockets over the limit close old sockets
	UserLimit int
}

type Udp4Buffer struct {
	UdpBuffer[BufferId4]
}

func NewUdp4Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
	udpBufferSettings *UdpBufferSettings) *Udp4Buffer {
	return &Udp4Buffer{
		UdpBuffer: *newUdpBuffer[BufferId4](ctx, receiveCallback, udpBufferSettings),
	}
}

func (self *Udp4Buffer) send(source TransferPath, provideMode protocol.ProvideMode,
	ipv4 *layers.IPv4, udp *layers.UDP, timeout time.Duration) (bool, error) {
	bufferId := NewBufferId4(
		source,
		ipv4.SrcIP, int(udp.SrcPort),
		ipv4.DstIP, int(udp.DstPort),
	)

	return self.udpSend(
		bufferId,
		ipv4.SrcIP,
		ipv4.DstIP,
		source,
		provideMode,
		4,
		udp,
		timeout,
	)
}

type Udp6Buffer struct {
	UdpBuffer[BufferId6]
}

func NewUdp6Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
	udpBufferSettings *UdpBufferSettings) *Udp6Buffer {
	return &Udp6Buffer{
		UdpBuffer: *newUdpBuffer[BufferId6](ctx, receiveCallback, udpBufferSettings),
	}
}

func (self *Udp6Buffer) send(source TransferPath, provideMode protocol.ProvideMode,
	ipv6 *layers.IPv6, udp *layers.UDP, timeout time.Duration) (bool, error) {
	bufferId := NewBufferId6(
		source,
		ipv6.SrcIP, int(udp.SrcPort),
		ipv6.DstIP, int(udp.DstPort),
	)

	return self.udpSend(
		bufferId,
		ipv6.SrcIP,
		ipv6.DstIP,
		source,
		provideMode,
		6,
		udp,
		timeout,
	)
}

type UdpBuffer[BufferId comparable] struct {
	ctx               context.Context
	receiveCallback   ReceivePacketFunction
	udpBufferSettings *UdpBufferSettings

	mutex sync.Mutex

	sequences       map[BufferId]*UdpSequence
	sourceSequences map[TransferPath]map[BufferId]*UdpSequence
}

func newUdpBuffer[BufferId comparable](
	ctx context.Context,
	receiveCallback ReceivePacketFunction,
	udpBufferSettings *UdpBufferSettings,
) *UdpBuffer[BufferId] {
	return &UdpBuffer[BufferId]{
		ctx:               ctx,
		receiveCallback:   receiveCallback,
		udpBufferSettings: udpBufferSettings,
		sequences:         map[BufferId]*UdpSequence{},
		sourceSequences:   map[TransferPath]map[BufferId]*UdpSequence{},
	}
}

func (self *UdpBuffer[BufferId]) udpSend(
	bufferId BufferId,
	sourceIp net.IP,
	destinationIp net.IP,
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipVersion int,
	udp *layers.UDP,
	timeout time.Duration,
) (bool, error) {
	initSequence := func(skip *UdpSequence) *UdpSequence {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		sequence, ok := self.sequences[bufferId]
		if ok {
			if skip == nil || skip != sequence {
				return sequence
			} else {
				sequence.Cancel()
				delete(self.sequences, bufferId)
				sourceSequences := self.sourceSequences[sequence.source]
				delete(sourceSequences, bufferId)
				if 0 == len(sourceSequences) {
					delete(self.sourceSequences, sequence.source)
				}
			}
		}

		if 0 < self.udpBufferSettings.UserLimit {
			// limit the total connections per source to avoid blowing up the ulimit
			if sourceSequences := self.sourceSequences[source]; self.udpBufferSettings.UserLimit < len(sourceSequences) {
				applyLruUserLimit(maps.Values(sourceSequences), self.udpBufferSettings.UserLimit, func(sequence *UdpSequence) bool {
					glog.Infof(
						"[lnr]udp limit source %s->%s\n",
						source,
						net.JoinHostPort(
							sequence.destinationIp.String(),
							strconv.Itoa(int(sequence.destinationPort)),
						),
					)
					return true
				})
			}
		}

		// TODO
		// limit the number of new connections per second per source
		// self.sourceLimiter[source].Limit()

		sequence = NewUdpSequence(
			self.ctx,
			self.receiveCallback,
			source,
			ipVersion,
			sourceIp,
			udp.SrcPort,
			destinationIp,
			udp.DstPort,
			self.udpBufferSettings,
		)
		self.sequences[bufferId] = sequence
		go func() {
			sequence.Run()

			self.mutex.Lock()
			defer self.mutex.Unlock()
			sequence.Close()
			// clean up
			if sequence == self.sequences[bufferId] {
				delete(self.sequences, bufferId)
				sourceSequences := self.sourceSequences[sequence.source]
				delete(sourceSequences, bufferId)
				if 0 == len(sourceSequences) {
					delete(self.sourceSequences, sequence.source)
				}
			}
		}()
		return sequence
	}

	sendItem := &UdpSendItem{
		provideMode: provideMode,
		udp:         udp,
	}
	sequence := initSequence(nil)
	if success, err := sequence.send(sendItem, timeout); err == nil {
		return success, nil
	} else {
		// sequence closed
		return initSequence(sequence).send(sendItem, timeout)
	}
}

type UdpSequence struct {
	ctx               context.Context
	cancel            context.CancelFunc
	receiveCallback   ReceivePacketFunction
	udpBufferSettings *UdpBufferSettings

	sendItems chan *UdpSendItem

	idleCondition *IdleCondition

	StreamState
}

func NewUdpSequence(ctx context.Context, receiveCallback ReceivePacketFunction,
	source TransferPath,
	ipVersion int,
	sourceIp net.IP, sourcePort layers.UDPPort,
	destinationIp net.IP, destinationPort layers.UDPPort,
	udpBufferSettings *UdpBufferSettings) *UdpSequence {
	cancelCtx, cancel := context.WithCancel(ctx)
	streamState := StreamState{
		source:          source,
		ipVersion:       ipVersion,
		sourceIp:        sourceIp,
		sourcePort:      sourcePort,
		destinationIp:   destinationIp,
		destinationPort: destinationPort,
		userLimited:     *newUserLimited(),
	}
	return &UdpSequence{
		ctx:               cancelCtx,
		cancel:            cancel,
		receiveCallback:   receiveCallback,
		sendItems:         make(chan *UdpSendItem, udpBufferSettings.SequenceBufferSize),
		udpBufferSettings: udpBufferSettings,
		idleCondition:     NewIdleCondition(),
		StreamState:       streamState,
	}
}

func (self *UdpSequence) send(sendItem *UdpSendItem, timeout time.Duration) (bool, error) {
	if !self.idleCondition.UpdateOpen() {
		return false, nil
	}
	defer self.idleCondition.UpdateClose()

	select {
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	if timeout < 0 {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.sendItems <- sendItem:
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.sendItems <- sendItem:
			return true, nil
		default:
			return false, nil
		}
	} else {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.sendItems <- sendItem:
			return true, nil
		case <-time.After(timeout):
			return false, nil
		}
	}
}

func (self *UdpSequence) Run() {
	defer self.cancel()

	receive := func(packet []byte) {
		self.receiveCallback(self.source, IpProtocolUdp, packet)
	}

	glog.V(2).Infof("[init]udp connect\n")
	socket, err := net.Dial(
		"udp",
		self.DestinationAuthority(),
	)
	if err != nil {
		glog.Infof("[init]udp connect error = %s\n", err)
		return
	}
	defer socket.Close()
	self.UpdateLastActivityTime()
	glog.V(2).Infof("[init]connect success\n")

	go func() {
		defer self.cancel()

		buffer := make([]byte, self.udpBufferSettings.ReadBufferByteCount)

		for forwardIter := uint64(0); ; forwardIter += 1 {
			select {
			case <-self.ctx.Done():
				return
			default:
			}

			readTimeout := time.Now().Add(self.udpBufferSettings.ReadTimeout)
			socket.SetReadDeadline(readTimeout)
			n, err := socket.Read(buffer)

			if err != nil {
				glog.Infof("[f%d]udp receive err = %s\n", forwardIter, err)
			}

			if 0 < n {
				self.UpdateLastActivityTime()

				packets, packetsErr := self.DataPackets(buffer, n, self.udpBufferSettings.Mtu)
				if packetsErr != nil {
					glog.Infof("[f%d]udp receive packets error = %s\n", forwardIter, packetsErr)
					return
				}
				if 1 < len(packets) {
					glog.V(2).Infof("[f%d]udp receive segemented packets = %d\n", forwardIter, len(packets))
				}
				for _, packet := range packets {
					glog.V(1).Infof("[f%d]udp receive %d\n", forwardIter, len(packet))
					receive(packet)
				}
			}

			if err != nil {
				if err == io.EOF {
					return
				} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					glog.Infof("[f%d]timeout\n", forwardIter)
					return
				} else {
					// some other error
					return
				}
			}
		}
	}()

	go func() {
		defer self.cancel()

		for sendIter := 0; ; sendIter += 1 {
			checkpointId := self.idleCondition.Checkpoint()
			select {
			case <-self.ctx.Done():
				return
			case sendItem := <-self.sendItems:
				writeEndTime := time.Now().Add(self.udpBufferSettings.WriteTimeout)

				payload := sendItem.udp.Payload

				for i := 0; i < len(payload); {
					select {
					case <-self.ctx.Done():
						return
					default:
					}

					socket.SetWriteDeadline(writeEndTime)
					n, err := socket.Write(payload[i:])

					if err == nil {
						glog.V(2).Infof("[f%d]udp forward %d\n", sendIter, n)
					} else {
						glog.Infof("[f%d]udp forward %d error = %s", sendIter, n, err)
					}

					if 0 < n {
						self.UpdateLastActivityTime()

						j := i
						i += n
						glog.V(2).Infof("[f%d]udp forward %d/%d -> %d/%d +%d\n", sendIter, j, len(payload), i, len(payload), n)
					}

					if err != nil {
						if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
							return
						} else {
							// some other error
							return
						}
					}
				}
			case <-time.After(self.udpBufferSettings.IdleTimeout):
				if self.idleCondition.Close(checkpointId) {
					// close the sequence
					return
				}
				// else there pending updates
			}
		}
	}()

	select {
	case <-self.ctx.Done():
	}
}

func (self *UdpSequence) Cancel() {
	self.cancel()
}

func (self *UdpSequence) Close() {
	self.cancel()
}

type UdpSendItem struct {
	source      TransferPath
	provideMode protocol.ProvideMode
	udp         *layers.UDP
}

type StreamState struct {
	source          TransferPath
	ipVersion       int
	sourceIp        net.IP
	sourcePort      layers.UDPPort
	destinationIp   net.IP
	destinationPort layers.UDPPort

	userLimited
}

func (self *StreamState) SourceAuthority() string {
	return net.JoinHostPort(
		self.sourceIp.String(),
		strconv.Itoa(int(self.sourcePort)),
	)
}

func (self *StreamState) DestinationAuthority() string {
	return net.JoinHostPort(
		self.destinationIp.String(),
		strconv.Itoa(int(self.destinationPort)),
	)
}

func (self *StreamState) DataPackets(payload []byte, n int, mtu int) ([][]byte, error) {
	headerSize := 0
	var ip gopacket.NetworkLayer
	switch self.ipVersion {
	case 4:
		ip = &layers.IPv4{
			Version:  4,
			TTL:      64,
			SrcIP:    self.destinationIp,
			DstIP:    self.sourceIp,
			Protocol: layers.IPProtocolUDP,
		}
		headerSize += Ipv4HeaderSizeWithoutExtensions
	case 6:
		ip = &layers.IPv6{
			Version:    6,
			HopLimit:   64,
			SrcIP:      self.destinationIp,
			DstIP:      self.sourceIp,
			NextHeader: layers.IPProtocolUDP,
		}
		headerSize += Ipv6HeaderSize
	}

	udp := layers.UDP{
		SrcPort: self.destinationPort,
		DstPort: self.sourcePort,
	}
	udp.SetNetworkLayerForChecksum(ip)
	headerSize += UdpHeaderSize

	options := gopacket.SerializeOptions{
		ComputeChecksums: true,
		FixLengths:       true,
	}

	if debugVerifyHeaders {
		buffer := gopacket.NewSerializeBufferExpectedSize(headerSize, 0)
		err := gopacket.SerializeLayers(buffer, options,
			ip.(gopacket.SerializableLayer),
			&udp,
		)
		if err != nil {
			return nil, err
		}
		packetHeaders := buffer.Bytes()
		if headerSize != len(packetHeaders) {
			return nil, errors.New(fmt.Sprintf("Header check failed %d <> %d", headerSize, len(packetHeaders)))
		}
	}

	if headerSize+n <= mtu {
		buffer := gopacket.NewSerializeBufferExpectedSize(headerSize+n, 0)
		err := gopacket.SerializeLayers(buffer, options,
			ip.(gopacket.SerializableLayer),
			&udp,
			gopacket.Payload(payload[0:n]),
		)
		if err != nil {
			return nil, err
		}
		packet := buffer.Bytes()
		return [][]byte{packet}, nil
	} else {
		// fragment
		buffer := gopacket.NewSerializeBufferExpectedSize(mtu, 0)
		packetSize := mtu - headerSize
		packets := make([][]byte, 0, (n+packetSize)/packetSize)
		for i := 0; i < n; {
			j := min(i+packetSize, n)
			err := gopacket.SerializeLayers(buffer, options,
				ip.(gopacket.SerializableLayer),
				&udp,
				gopacket.Payload(payload[i:j]),
			)
			if err != nil {
				return nil, err
			}
			packet := buffer.Bytes()
			packetCopy := make([]byte, len(packet))
			copy(packetCopy, packet)
			packets = append(packets, packetCopy)
			buffer.Clear()
			i = j
		}
		return packets, nil
	}
}

type TcpBufferSettings struct {
	ConnectTimeout     time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	AckCompressTimeout time.Duration
	// ReadPollTimeout time.Duration
	// WritePollTimeout time.Duration
	IdleTimeout         time.Duration
	ReadBufferByteCount int
	SequenceBufferSize  int
	Mtu                 int
	// the window size is the max amount of packet data in memory for each sequence
	// `WindowSize / 2^WindowScale` must fit in uint16
	// see https://datatracker.ietf.org/doc/html/rfc1323#page-8
	WindowScale uint32
	WindowSize  uint32
	// the number of open sockets per user
	// uses an lru cleanup where new sockets over the limit close old sockets
	UserLimit int
}

type Tcp4Buffer struct {
	TcpBuffer[BufferId4]
}

func NewTcp4Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
	tcpBufferSettings *TcpBufferSettings) *Tcp4Buffer {
	return &Tcp4Buffer{
		TcpBuffer: *newTcpBuffer[BufferId4](ctx, receiveCallback, tcpBufferSettings),
	}
}

func (self *Tcp4Buffer) send(source TransferPath, provideMode protocol.ProvideMode,
	ipv4 *layers.IPv4, tcp *layers.TCP, timeout time.Duration) (bool, error) {
	bufferId := NewBufferId4(
		source,
		ipv4.SrcIP, int(tcp.SrcPort),
		ipv4.DstIP, int(tcp.DstPort),
	)

	return self.tcpSend(
		bufferId,
		ipv4.SrcIP,
		ipv4.DstIP,
		source,
		provideMode,
		4,
		tcp,
		timeout,
	)
}

type Tcp6Buffer struct {
	TcpBuffer[BufferId6]
}

func NewTcp6Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
	tcpBufferSettings *TcpBufferSettings) *Tcp6Buffer {
	return &Tcp6Buffer{
		TcpBuffer: *newTcpBuffer[BufferId6](ctx, receiveCallback, tcpBufferSettings),
	}
}

func (self *Tcp6Buffer) send(source TransferPath, provideMode protocol.ProvideMode,
	ipv6 *layers.IPv6, tcp *layers.TCP, timeout time.Duration) (bool, error) {
	bufferId := NewBufferId6(
		source,
		ipv6.SrcIP, int(tcp.SrcPort),
		ipv6.DstIP, int(tcp.DstPort),
	)

	return self.tcpSend(
		bufferId,
		ipv6.SrcIP,
		ipv6.DstIP,
		source,
		provideMode,
		6,
		tcp,
		timeout,
	)
}

type TcpBuffer[BufferId comparable] struct {
	ctx               context.Context
	receiveCallback   ReceivePacketFunction
	tcpBufferSettings *TcpBufferSettings

	mutex sync.Mutex

	sequences       map[BufferId]*TcpSequence
	sourceSequences map[TransferPath]map[BufferId]*TcpSequence
}

func newTcpBuffer[BufferId comparable](
	ctx context.Context,
	receiveCallback ReceivePacketFunction,
	tcpBufferSettings *TcpBufferSettings,
) *TcpBuffer[BufferId] {
	return &TcpBuffer[BufferId]{
		ctx:               ctx,
		receiveCallback:   receiveCallback,
		tcpBufferSettings: tcpBufferSettings,
		sequences:         map[BufferId]*TcpSequence{},
		sourceSequences:   map[TransferPath]map[BufferId]*TcpSequence{},
	}
}

func (self *TcpBuffer[BufferId]) tcpSend(
	bufferId BufferId,
	sourceIp net.IP,
	destinationIp net.IP,
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipVersion int,
	tcp *layers.TCP,
	timeout time.Duration,
) (bool, error) {
	initSequence := func() *TcpSequence {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		if !tcp.SYN {
			if sequence, ok := self.sequences[bufferId]; ok {
				return sequence
			}
			// drop the packet; only create a new sequence on SYN
			glog.V(2).Infof("[lnr]tcp drop no syn (%s)\n", tcpFlagsString(tcp))
			return nil
		}

		// else new sequence
		if sequence, ok := self.sequences[bufferId]; ok {
			sequence.Cancel()
			delete(self.sequences, bufferId)
			sourceSequences := self.sourceSequences[sequence.source]
			delete(sourceSequences, bufferId)
			if 0 == len(sourceSequences) {
				delete(self.sourceSequences, sequence.source)
			}
		}

		if 0 < self.tcpBufferSettings.UserLimit {
			// limit the total connections per source to avoid blowing up the ulimit
			if sourceSequences := self.sourceSequences[source]; self.tcpBufferSettings.UserLimit < len(sourceSequences) {
				applyLruUserLimit(maps.Values(sourceSequences), self.tcpBufferSettings.UserLimit, func(sequence *TcpSequence) bool {
					glog.Infof(
						"[lnr]tcp limit source %s->%s\n",
						source,
						net.JoinHostPort(
							sequence.destinationIp.String(),
							strconv.Itoa(int(sequence.destinationPort)),
						),
					)
					return true
				})
			}
		}

		// TODO
		// limit the number of new connections per second per source
		// self.sourceLimiter[source].Limit()

		sequence := NewTcpSequence(
			self.ctx,
			self.receiveCallback,
			source,
			ipVersion,
			sourceIp,
			tcp.SrcPort,
			destinationIp,
			tcp.DstPort,
			self.tcpBufferSettings,
		)
		self.sequences[bufferId] = sequence
		go func() {
			sequence.Run()

			self.mutex.Lock()
			defer self.mutex.Unlock()
			sequence.Close()
			// clean up
			if sequence == self.sequences[bufferId] {
				delete(self.sequences, bufferId)
				sourceSequences := self.sourceSequences[sequence.source]
				delete(sourceSequences, bufferId)
				if 0 == len(sourceSequences) {
					delete(self.sourceSequences, sequence.source)
				}
			}
		}()
		return sequence
	}
	sendItem := &TcpSendItem{
		provideMode: provideMode,
		tcp:         tcp,
	}
	if sequence := initSequence(); sequence == nil {
		// sequence does not exist and not a syn packet, drop
		return false, nil
	} else {
		return sequence.send(sendItem, timeout)
	}
}

/*
** Important implementation note **
In this implementation, packet flow from the UNAT to the source
is assumed to never require retransmits. The retrasmit logic
is not implemented.
This is a safe assumption when moving packets from local raw socket
to the UNAT via `transfer`, which is lossless and in-order.
*/
type TcpSequence struct {
	ctx    context.Context
	cancel context.CancelFunc

	receiveCallback ReceivePacketFunction

	tcpBufferSettings *TcpBufferSettings

	sendItems chan *TcpSendItem

	idleCondition *IdleCondition

	ConnectionState
}

func NewTcpSequence(ctx context.Context, receiveCallback ReceivePacketFunction,
	source TransferPath,
	ipVersion int,
	sourceIp net.IP, sourcePort layers.TCPPort,
	destinationIp net.IP, destinationPort layers.TCPPort,
	tcpBufferSettings *TcpBufferSettings) *TcpSequence {
	cancelCtx, cancel := context.WithCancel(ctx)

	connectionState := ConnectionState{
		source:          source,
		ipVersion:       ipVersion,
		sourceIp:        sourceIp,
		sourcePort:      sourcePort,
		destinationIp:   destinationIp,
		destinationPort: destinationPort,
		// the window size starts at the fixed value
		enableWindowScale: false,
		windowSize:        tcpBufferSettings.WindowSize,
		windowScale:       0,
		userLimited:       *newUserLimited(),
	}
	return &TcpSequence{
		ctx:               cancelCtx,
		cancel:            cancel,
		receiveCallback:   receiveCallback,
		tcpBufferSettings: tcpBufferSettings,
		sendItems:         make(chan *TcpSendItem, tcpBufferSettings.SequenceBufferSize),
		idleCondition:     NewIdleCondition(),
		ConnectionState:   connectionState,
	}
}

func (self *TcpSequence) send(sendItem *TcpSendItem, timeout time.Duration) (bool, error) {
	if !self.idleCondition.UpdateOpen() {
		return false, nil
	}
	defer self.idleCondition.UpdateClose()

	select {
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	if timeout < 0 {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.sendItems <- sendItem:
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.sendItems <- sendItem:
			return true, nil
		default:
			return false, nil
		}
	} else {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.sendItems <- sendItem:
			return true, nil
		case <-time.After(timeout):
			return false, nil
		}
	}
}

func (self *TcpSequence) Run() {
	defer self.cancel()

	// note receive is called from multiple go routines
	// tcp packets with ack may be reordered due to being written in parallel
	receive := func(packet []byte) {
		self.receiveCallback(self.source, IpProtocolTcp, packet)
	}

	closed := false
	// send a final FIN+ACK
	defer func() {
		if closed {
			glog.V(2).Infof("[r]closed gracefully\n")
		} else {
			glog.V(2).Infof("[r]closed unexpected sending RST\n")
			var packet []byte
			var err error
			func() {
				self.mutex.Lock()
				defer self.mutex.Unlock()

				packet, err = self.RstAck()
			}()
			if err == nil {
				receive(packet)
			}
		}
	}()

	for syn := false; !syn; {
		select {
		case <-self.ctx.Done():
			return
		case sendItem := <-self.sendItems:
			glog.V(2).Infof("[init]send(%d)\n", len(sendItem.tcp.BaseLayer.Payload))
			// the first packet must be a syn
			if sendItem.tcp.SYN {
				glog.V(2).Infof("[init]SYN\n")

				var packet []byte
				var err error
				func() {
					self.mutex.Lock()
					defer self.mutex.Unlock()

					// sendSeq is the next expected sequence number
					// SYN and FIN consume one
					self.sendSeq = sendItem.tcp.Seq + 1
					// start the send seq at send seq
					// this is arbitrary, and since there is no transport security risk back to sender is fine
					self.receiveSeq = sendItem.tcp.Seq
					self.receiveSeqAck = sendItem.tcp.Seq

					parseWindowScaleOpts := func() (bool, uint32) {
						for _, opt := range sendItem.tcp.Options {
							if opt.OptionType == layers.TCPOptionKindWindowScale {
								// fmt.Printf("[init]OPTION DATA = %v (%d,%d)\n", opt.OptionData, len(opt.OptionData), opt.OptionLength)
								windowScaleBytes := make([]byte, 4)
								copy(windowScaleBytes[4-len(opt.OptionData):4], opt.OptionData)
								windowScale := min(
									binary.BigEndian.Uint32(windowScaleBytes[0:4]),
									// see 2.3  Using the Window Scale Option
									14,
								)
								return true, windowScale
							}
						}
						return false, 0
					}

					self.enableWindowScale, self.receiveWindowScale = parseWindowScaleOpts()
					self.receiveWindowSize = uint32(sendItem.tcp.Window) << self.receiveWindowScale
					if self.enableWindowScale {
						// compute the window scale to fit the window size in uint16
						bits := math.Log2(float64(self.windowSize) / float64(math.MaxUint16))
						if 0 <= bits {
							self.windowScale = uint32(math.Ceil(bits))
						} else {
							self.windowScale = 0
						}
					} else {
						// turn off window scale for send
						self.windowScale = 0
					}
					glog.V(2).Infof("[init]window=%d/%d, receive=%d/%d\n", self.windowSize, self.windowScale, self.receiveWindowSize, self.receiveWindowScale)
					self.encodedWindowSize = uint16(min(
						uint32(self.windowSize>>self.windowScale),
						uint32(math.MaxUint16),
					))

					packet, err = self.SynAck()
					self.receiveSeq += 1
				}()
				if err == nil {
					glog.V(2).Infof("[init]receive SYN+ACK\n")
					receive(packet)
				}

				syn = true
			} else {
				// an ACK here could be for a previous FIN
				glog.V(2).Infof("[init]waiting for SYN (%s)\n", tcpFlagsString(sendItem.tcp))
			}
		}
	}

	glog.V(2).Infof("[init]tcp connect\n")
	socket, err := net.DialTimeout(
		"tcp",
		self.DestinationAuthority(),
		self.tcpBufferSettings.ConnectTimeout,
	)
	if err != nil {
		glog.Infof("[init]tcp connect error = %s\n", err)
		return
	}
	defer socket.Close()

	/*
		if v, ok := socket.(*net.TCPConn); ok {
			if err := v.SetWriteBuffer(int(self.windowSize)); err != nil {
				glog.Infof("[init]could not set write buffer = %d\n", self.windowSize)
			}
			// if err := v.SetReadBuffer(int(self.receiveWindowSize)); err != nil {
			// 	glog.Infof("[init]could not set read buffer = %d\n", self.receiveWindowSize)
			// }
		}
	*/

	self.UpdateLastActivityTime()
	glog.V(2).Infof("[init]connect success\n")

	receiveAckCond := sync.NewCond(&self.mutex)
	ackCond := sync.NewCond(&self.mutex)
	defer func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		receiveAckCond.Broadcast()
		ackCond.Broadcast()
	}()

	var ackedSendSeq uint32
	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		ackedSendSeq = self.sendSeq
	}()

	go func() {
		defer self.cancel()

		buffer := make([]byte, self.tcpBufferSettings.ReadBufferByteCount)

		for forwardIter := uint64(0); ; forwardIter += 1 {
			select {
			case <-self.ctx.Done():
				return
			default:
			}

			readTimeout := time.Now().Add(self.tcpBufferSettings.ReadTimeout)
			socket.SetReadDeadline(readTimeout)

			n, err := socket.Read(buffer)

			if err != nil {
				glog.Infof("[f%d]tcp receive error = %s\n", forwardIter, err)
			}

			if 0 < n {
				self.UpdateLastActivityTime()

				// since the transfer from local to remove is lossless and preserves order,
				// do not worry about retransmits
				var packets [][]byte
				var packetsErr error
				func() {
					self.mutex.Lock()
					defer self.mutex.Unlock()

					select {
					case <-self.ctx.Done():
						return
					default:
					}

					for uint32(self.receiveWindowSize) < self.receiveSeq-self.receiveSeqAck+uint32(n) {
						glog.V(2).Infof("[f%d]tcp receive window wait\n", forwardIter)
						receiveAckCond.Wait()
						select {
						case <-self.ctx.Done():
							return
						default:
						}
					}

					packets, packetsErr = self.DataPackets(buffer, n, self.tcpBufferSettings.Mtu)
					if packetsErr != nil {
						glog.Infof("[f%d]tcp receive packets error = %s\n", forwardIter, packetsErr)
						return
					}

					if 1 < len(packets) {
						glog.V(2).Infof("[f%d]tcp receive segmented packets %d\n", forwardIter, len(packets))
					}
					glog.V(2).Infof("[f%d]tcp receive %d %d %d\n", forwardIter, n, len(packets), self.receiveSeq)

					self.receiveSeq += uint32(n)

					ackedSendSeq = self.sendSeq
				}()
				if packets == nil {
					return
				}

				select {
				case <-self.ctx.Done():
					return
				default:
				}

				for _, packet := range packets {
					receive(packet)
				}
			}

			if err != nil {
				if err == io.EOF {
					// closed (FIN)
					// propagate the FIN and close the sequence
					glog.V(2).Infof("[final]FIN\n")
					var packet []byte
					var err error
					func() {
						self.mutex.Lock()
						defer self.mutex.Unlock()

						packet, err = self.FinAck()
						self.receiveSeq += 1
					}()
					if err == nil {
						closed = true
						receive(packet)
					}
					return
				} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					glog.V(2).Infof("[f%d]timeout\n", forwardIter)
					return
				} else {
					// some other error
					return
				}
			}
		}
	}()

	type writePayload struct {
		sendIter uint64
		payload  []byte
	}

	writePayloads := make(chan writePayload, self.tcpBufferSettings.SequenceBufferSize)
	go func() {
		defer self.cancel()

		for {
			select {
			case <-self.ctx.Done():
				return
			case writePayload := <-writePayloads:
				payload := writePayload.payload
				sendIter := writePayload.sendIter
				writeEndTime := time.Now().Add(self.tcpBufferSettings.WriteTimeout)
				for i := 0; i < len(payload); {
					select {
					case <-self.ctx.Done():
						return
					default:
					}

					socket.SetWriteDeadline(writeEndTime)
					n, err := socket.Write(payload[i:])

					if err == nil {
						glog.V(2).Infof("[f%d]tcp forward %d\n", sendIter, n)
					} else {
						glog.Infof("[f%d]tcp forward %d error = %s\n", sendIter, n, err)
					}

					if 0 < n {
						self.UpdateLastActivityTime()

						j := i
						i += n
						glog.V(2).Infof("[f%d]tcp forward %d/%d -> %d/%d +%d\n", sendIter, j, len(payload), i, len(payload), n)
					}

					if err != nil {
						if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
							return
						} else {
							// some other error
							return
						}
					}
				}

			}
		}
	}()

	go func() {
		defer self.cancel()

		for {
			select {
			case <-self.ctx.Done():
				return
			default:
			}

			var packet []byte
			func() {
				self.mutex.Lock()
				defer self.mutex.Unlock()

				select {
				case <-self.ctx.Done():
					return
				default:
				}

				for self.sendSeq == ackedSendSeq {
					ackCond.Wait()
					select {
					case <-self.ctx.Done():
						return
					default:
					}
				}

				var err error
				packet, err = self.PureAck()
				if err != nil {
					glog.Infof("[r]ack err = %s\n", err)
				}
				ackedSendSeq = self.sendSeq
			}()
			if packet == nil {
				return
			}

			select {
			case <-self.ctx.Done():
				return
			default:
			}

			receive(packet)

			if 0 < self.tcpBufferSettings.AckCompressTimeout {
				select {
				case <-time.After(self.tcpBufferSettings.AckCompressTimeout):
				case <-self.ctx.Done():
					return
				}
			}
		}
	}()

	for sendIter := uint64(0); ; sendIter += 1 {
		checkpointId := self.idleCondition.Checkpoint()
		select {
		case <-self.ctx.Done():
			return
		case sendItem := <-self.sendItems:
			if glog.V(2) {
				if "ACK" != tcpFlagsString(sendItem.tcp) {
					glog.Infof("[r%d]receive(%d %s)\n", sendIter, len(sendItem.tcp.Payload), tcpFlagsString(sendItem.tcp))
				}
			}

			drop := false
			seq := uint32(0)

			func() {
				self.mutex.Lock()
				defer self.mutex.Unlock()

				if self.sendSeq != sendItem.tcp.Seq {
					// a retransmit
					// since the transfer from local to remote is lossless and preserves order,
					// the packet is already pending. Ignore.
					drop = true
				} else if sendItem.tcp.ACK {
					// acks are reliably delivered (see above)
					// we do not need to resend receive packets on missing acks
					// note the window size can be be adjusted at any time for the same receive seq number,
					// e.g. ->0 then ->full on receiver full
					if self.receiveSeqAck <= sendItem.tcp.Ack {
						self.receiveWindowSize = uint32(sendItem.tcp.Window) << self.receiveWindowScale
						self.receiveSeqAck = sendItem.tcp.Ack
						receiveAckCond.Broadcast()
					}
				}
			}()

			if drop {
				continue
			}

			if sendItem.tcp.FIN {
				glog.V(2).Infof("[r%d]FIN\n", sendIter)
				seq += 1
			}

			payload := sendItem.tcp.Payload
			if 0 < len(payload) {
				seq += uint32(len(payload))
				writePayload := writePayload{
					payload:  payload,
					sendIter: sendIter,
				}
				select {
				case <-self.ctx.Done():
					return
				case writePayloads <- writePayload:
				}
			}

			if 0 < seq {
				func() {
					self.mutex.Lock()
					defer self.mutex.Unlock()

					self.sendSeq += seq
					ackCond.Broadcast()
				}()
			}

			if sendItem.tcp.FIN {
				// close the socket to propage the FIN and close the sequence
				socket.Close()
			}

			if sendItem.tcp.RST {
				// a RST typically appears for a bad TCP segment
				glog.V(2).Infof("[r%d]RST\n", sendIter)
				return
			}

		case <-time.After(self.tcpBufferSettings.IdleTimeout):
			if self.idleCondition.Close(checkpointId) {
				// close the sequence
				glog.V(2).Infof("[r%d]timeout\n", sendIter)
				return
			}
			// else there pending updates
		}
	}
}

func (self *TcpSequence) Cancel() {
	self.cancel()
}

func (self *TcpSequence) Close() {
	self.cancel()
}

type TcpSendItem struct {
	provideMode protocol.ProvideMode
	tcp         *layers.TCP
}

type ConnectionState struct {
	source          TransferPath
	ipVersion       int
	sourceIp        net.IP
	sourcePort      layers.TCPPort
	destinationIp   net.IP
	destinationPort layers.TCPPort

	mutex sync.Mutex

	sendSeq            uint32
	receiveSeq         uint32
	receiveSeqAck      uint32
	receiveWindowSize  uint32
	receiveWindowScale uint32
	enableWindowScale  bool
	windowSize         uint32
	windowScale        uint32
	encodedWindowSize  uint16

	userLimited
}

func (self *ConnectionState) SourceAuthority() string {
	return net.JoinHostPort(
		self.sourceIp.String(),
		strconv.Itoa(int(self.sourcePort)),
	)
}

func (self *ConnectionState) DestinationAuthority() string {
	return net.JoinHostPort(
		self.destinationIp.String(),
		strconv.Itoa(int(self.destinationPort)),
	)
}

func (self *ConnectionState) SynAck() ([]byte, error) {
	headerSize := 0
	var ip gopacket.NetworkLayer
	switch self.ipVersion {
	case 4:
		ip = &layers.IPv4{
			Version:  4,
			TTL:      64,
			SrcIP:    self.destinationIp,
			DstIP:    self.sourceIp,
			Protocol: layers.IPProtocolTCP,
		}
		headerSize += Ipv4HeaderSizeWithoutExtensions
	case 6:
		ip = &layers.IPv6{
			Version:    6,
			HopLimit:   64,
			SrcIP:      self.destinationIp,
			DstIP:      self.sourceIp,
			NextHeader: layers.IPProtocolTCP,
		}
		headerSize += Ipv6HeaderSize
	}

	opts := []layers.TCPOption{}

	if self.enableWindowScale {
		windowScaleBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(windowScaleBytes[0:4], self.windowScale)

		windowScaleOpt := layers.TCPOption{
			OptionType:   layers.TCPOptionKindWindowScale,
			OptionLength: 3,
			// one byte
			OptionData: windowScaleBytes[3:4],
		}
		opts = append(opts, windowScaleOpt)
	}

	tcp := layers.TCP{
		SrcPort: self.destinationPort,
		DstPort: self.sourcePort,
		Seq:     self.receiveSeq,
		Ack:     self.sendSeq,
		ACK:     true,
		SYN:     true,
		Window:  self.encodedWindowSize,
		Options: opts,
	}
	tcp.SetNetworkLayerForChecksum(ip)
	headerSize += TcpHeaderSizeWithoutExtensions

	options := gopacket.SerializeOptions{
		ComputeChecksums: true,
		FixLengths:       true,
	}

	buffer := gopacket.NewSerializeBufferExpectedSize(headerSize, 0)

	err := gopacket.SerializeLayers(buffer, options,
		ip.(gopacket.SerializableLayer),
		&tcp,
	)

	if err != nil {
		return nil, err
	}
	packet := buffer.Bytes()
	return packet, nil
}

func (self *ConnectionState) PureAck() ([]byte, error) {
	headerSize := 0
	var ip gopacket.NetworkLayer
	switch self.ipVersion {
	case 4:
		ip = &layers.IPv4{
			Version:  4,
			TTL:      64,
			SrcIP:    self.destinationIp,
			DstIP:    self.sourceIp,
			Protocol: layers.IPProtocolTCP,
		}
		headerSize += Ipv4HeaderSizeWithoutExtensions
	case 6:
		ip = &layers.IPv6{
			Version:    6,
			HopLimit:   64,
			SrcIP:      self.destinationIp,
			DstIP:      self.sourceIp,
			NextHeader: layers.IPProtocolTCP,
		}
		headerSize += Ipv6HeaderSize
	}

	tcp := layers.TCP{
		SrcPort: self.destinationPort,
		DstPort: self.sourcePort,
		Seq:     self.receiveSeq,
		Ack:     self.sendSeq,
		ACK:     true,
		Window:  self.encodedWindowSize,
	}
	tcp.SetNetworkLayerForChecksum(ip)
	headerSize += TcpHeaderSizeWithoutExtensions

	options := gopacket.SerializeOptions{
		ComputeChecksums: true,
		FixLengths:       true,
	}

	buffer := gopacket.NewSerializeBufferExpectedSize(headerSize, 0)

	err := gopacket.SerializeLayers(buffer, options,
		ip.(gopacket.SerializableLayer),
		&tcp,
	)

	if err != nil {
		return nil, err
	}
	packet := buffer.Bytes()
	return packet, nil
}

func (self *ConnectionState) FinAck() ([]byte, error) {
	headerSize := 0
	var ip gopacket.NetworkLayer
	switch self.ipVersion {
	case 4:
		ip = &layers.IPv4{
			Version:  4,
			TTL:      64,
			SrcIP:    self.destinationIp,
			DstIP:    self.sourceIp,
			Protocol: layers.IPProtocolTCP,
		}
		headerSize += Ipv4HeaderSizeWithoutExtensions
	case 6:
		ip = &layers.IPv6{
			Version:    6,
			HopLimit:   64,
			SrcIP:      self.destinationIp,
			DstIP:      self.sourceIp,
			NextHeader: layers.IPProtocolTCP,
		}
		headerSize += Ipv6HeaderSize
	}

	tcp := layers.TCP{
		SrcPort: self.destinationPort,
		DstPort: self.sourcePort,
		Seq:     self.receiveSeq,
		Ack:     self.sendSeq,
		ACK:     true,
		FIN:     true,
		Window:  self.encodedWindowSize,
	}
	tcp.SetNetworkLayerForChecksum(ip)
	headerSize += TcpHeaderSizeWithoutExtensions

	options := gopacket.SerializeOptions{
		ComputeChecksums: true,
		FixLengths:       true,
	}

	buffer := gopacket.NewSerializeBufferExpectedSize(headerSize, 0)

	err := gopacket.SerializeLayers(buffer, options,
		ip.(gopacket.SerializableLayer),
		&tcp,
	)

	if err != nil {
		return nil, err
	}
	packet := buffer.Bytes()
	return packet, nil
}

func (self *ConnectionState) RstAck() ([]byte, error) {
	headerSize := 0
	var ip gopacket.NetworkLayer
	switch self.ipVersion {
	case 4:
		ip = &layers.IPv4{
			Version:  4,
			TTL:      64,
			SrcIP:    self.destinationIp,
			DstIP:    self.sourceIp,
			Protocol: layers.IPProtocolTCP,
		}
		headerSize += Ipv4HeaderSizeWithoutExtensions
	case 6:
		ip = &layers.IPv6{
			Version:    6,
			HopLimit:   64,
			SrcIP:      self.destinationIp,
			DstIP:      self.sourceIp,
			NextHeader: layers.IPProtocolTCP,
		}
		headerSize += Ipv6HeaderSize
	}

	tcp := layers.TCP{
		SrcPort: self.destinationPort,
		DstPort: self.sourcePort,
		Seq:     self.receiveSeq,
		Ack:     self.sendSeq,
		ACK:     true,
		RST:     true,
		Window:  self.encodedWindowSize,
	}
	tcp.SetNetworkLayerForChecksum(ip)
	headerSize += TcpHeaderSizeWithoutExtensions

	options := gopacket.SerializeOptions{
		ComputeChecksums: true,
		FixLengths:       true,
	}

	buffer := gopacket.NewSerializeBufferExpectedSize(headerSize, 0)

	err := gopacket.SerializeLayers(buffer, options,
		ip.(gopacket.SerializableLayer),
		&tcp,
	)

	if err != nil {
		return nil, err
	}
	packet := buffer.Bytes()
	return packet, nil
}

func (self *ConnectionState) DataPackets(payload []byte, n int, mtu int) ([][]byte, error) {
	headerSize := 0
	var ip gopacket.NetworkLayer
	switch self.ipVersion {
	case 4:
		ip = &layers.IPv4{
			Version:  4,
			TTL:      64,
			SrcIP:    self.destinationIp,
			DstIP:    self.sourceIp,
			Protocol: layers.IPProtocolTCP,
		}
		headerSize += Ipv4HeaderSizeWithoutExtensions
	case 6:
		ip = &layers.IPv6{
			Version:    6,
			HopLimit:   64,
			SrcIP:      self.destinationIp,
			DstIP:      self.sourceIp,
			NextHeader: layers.IPProtocolTCP,
		}
		headerSize += Ipv6HeaderSize
	}

	tcp := layers.TCP{
		SrcPort: self.destinationPort,
		DstPort: self.sourcePort,
		Seq:     self.receiveSeq,
		Ack:     self.sendSeq,
		ACK:     true,
		Window:  self.encodedWindowSize,
	}
	tcp.SetNetworkLayerForChecksum(ip)
	headerSize += TcpHeaderSizeWithoutExtensions

	options := gopacket.SerializeOptions{
		ComputeChecksums: true,
		FixLengths:       true,
	}

	if debugVerifyHeaders {
		buffer := gopacket.NewSerializeBufferExpectedSize(headerSize, 0)
		err := gopacket.SerializeLayers(buffer, options,
			ip.(gopacket.SerializableLayer),
			&tcp,
		)
		if err != nil {
			return nil, err
		}
		packetHeaders := buffer.Bytes()
		if headerSize != len(packetHeaders) {
			return nil, errors.New(fmt.Sprintf("Header check failed %d <> %d", headerSize, len(packetHeaders)))
		}
	}

	if headerSize+n <= mtu {
		buffer := gopacket.NewSerializeBufferExpectedSize(headerSize+n, 0)
		err := gopacket.SerializeLayers(buffer, options,
			ip.(gopacket.SerializableLayer),
			&tcp,
			gopacket.Payload(payload[0:n]),
		)
		if err != nil {
			return nil, err
		}
		packet := buffer.Bytes()
		return [][]byte{packet}, nil
	} else {
		// fragment
		buffer := gopacket.NewSerializeBufferExpectedSize(mtu, 0)
		packetSize := mtu - headerSize
		packets := [][]byte{}
		for i := 0; i < n; {
			j := min(i+packetSize, n)
			tcp.Seq = self.receiveSeq + uint32(i)
			err := gopacket.SerializeLayers(buffer, options,
				ip.(gopacket.SerializableLayer),
				&tcp,
				gopacket.Payload(payload[i:j]),
			)
			if err != nil {
				return nil, err
			}
			packet := buffer.Bytes()
			packetCopy := make([]byte, len(packet))
			copy(packetCopy, packet)
			packets = append(packets, packetCopy)
			buffer.Clear()
			i = j
		}
		return packets, nil
	}
}

func tcpFlagsString(tcp *layers.TCP) string {
	flags := []string{}
	if tcp.FIN {
		flags = append(flags, "FIN")
	}
	if tcp.SYN {
		flags = append(flags, "SYN")
	}
	if tcp.RST {
		flags = append(flags, "RST")
	}
	if tcp.PSH {
		flags = append(flags, "PSH")
	}
	if tcp.ACK {
		flags = append(flags, "ACK")
	}
	if tcp.URG {
		flags = append(flags, "URG")
	}
	if tcp.ECE {
		flags = append(flags, "ECE")
	}
	if tcp.CWR {
		flags = append(flags, "CWR")
	}
	if tcp.NS {
		flags = append(flags, "NS")
	}
	return strings.Join(flags, ", ")
}

func DefaultRemoteUserNatProviderSettings() *RemoteUserNatProviderSettings {
	return &RemoteUserNatProviderSettings{
		WriteTimeout: 30 * time.Second,
	}
}

type RemoteUserNatProviderSettings struct {
	WriteTimeout time.Duration
}

type RemoteUserNatProvider struct {
	client            *Client
	localUserNat      *LocalUserNat
	securityPolicy    *SecurityPolicy
	settings          *RemoteUserNatProviderSettings
	localUserNatUnsub func()
	clientUnsub       func()
}

func NewRemoteUserNatProviderWithDefaults(
	client *Client,
	localUserNat *LocalUserNat,
) *RemoteUserNatProvider {
	return NewRemoteUserNatProvider(client, localUserNat, DefaultRemoteUserNatProviderSettings())
}

func NewRemoteUserNatProvider(
	client *Client,
	localUserNat *LocalUserNat,
	settings *RemoteUserNatProviderSettings,
) *RemoteUserNatProvider {
	userNatProvider := &RemoteUserNatProvider{
		client:         client,
		localUserNat:   localUserNat,
		securityPolicy: DefaultSecurityPolicy(),
		settings:       settings,
	}

	localUserNatUnsub := localUserNat.AddReceivePacketCallback(userNatProvider.Receive)
	userNatProvider.localUserNatUnsub = localUserNatUnsub
	clientUnsub := client.AddReceiveCallback(userNatProvider.ClientReceive)
	userNatProvider.clientUnsub = clientUnsub

	return userNatProvider
}

// `ReceivePacketFunction`
func (self *RemoteUserNatProvider) Receive(source TransferPath, ipProtocol IpProtocol, packet []byte) {
	if self.client.ClientId() == source.SourceId {
		// locally generated traffic should use a separate local user nat
		glog.V(2).Infof("drop remote user nat provider s packet ->%s\n", source.SourceId)
		return
	}

	ipPacketFromProvider := &protocol.IpPacketFromProvider{
		IpPacket: &protocol.IpPacket{
			PacketBytes: packet,
		},
	}
	frame, err := ToFrame(ipPacketFromProvider)
	if err != nil {
		glog.V(2).Infof("drop remote user nat provider s packet ->%s = %s\n", source.SourceId, err)
		panic(err)
	}

	opts := []any{
		CompanionContract(),
	}
	switch ipProtocol {
	case IpProtocolUdp:
		opts = append(opts, NoAck())
		c := func() bool {
			// ack := make(chan error)
			sent := self.client.SendWithTimeout(
				frame,
				source.Reverse(),
				func(err error) {},
				self.settings.WriteTimeout,
				opts...,
			)
			return sent
		}
		if glog.V(2) {
			TraceWithReturn(
				fmt.Sprintf("[unps]udp %s->%s s(%s)", self.client.ClientTag(), source.SourceId, source.StreamId),
				c,
			)
		} else {
			c()
		}
	case IpProtocolTcp:
		c := func() bool {
			return self.client.SendWithTimeout(
				frame,
				source.Reverse(),
				func(err error) {},
				self.settings.WriteTimeout,
				opts...,
			)
		}
		if glog.V(2) {
			TraceWithReturn(
				fmt.Sprintf("[unps]tcp %s->%s s(%s)", self.client.ClientTag(), source.SourceId, source.StreamId),
				c,
			)
		} else {
			c()
		}
	}

}

// `connect.ReceiveFunction`
func (self *RemoteUserNatProvider) ClientReceive(source TransferPath, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
	for _, frame := range frames {
		switch frame.MessageType {
		case protocol.MessageType_IpIpPacketToProvider:
			ipPacketToProvider_, err := FromFrame(frame)
			if err != nil {
				panic(err)
			}
			ipPacketToProvider := ipPacketToProvider_.(*protocol.IpPacketToProvider)

			packet := ipPacketToProvider.IpPacket.PacketBytes
			_, r, err := self.securityPolicy.Inspect(provideMode, packet)
			if err == nil {
				switch r {
				case SecurityPolicyResultAllow:
					c := func() bool {
						return self.localUserNat.SendPacketWithTimeout(source, provideMode, packet, self.settings.WriteTimeout)
					}
					if glog.V(2) {
						TraceWithReturn(
							fmt.Sprintf("[unpr] %s<-%s s(%s)", self.client.ClientTag(), source.SourceId, source.StreamId),
							c,
						)
					} else {
						c()
					}
				case SecurityPolicyResultIncident:
					self.client.ReportAbuse(source)
				}
			}
		}
	}
}

func (self *RemoteUserNatProvider) Close() {
	// self.client.RemoveReceiveCallback(self.clientCallbackId)
	// self.localUserNat.RemoveReceivePacketCallback(self.localUserNatCallbackId)
	self.clientUnsub()
	self.localUserNatUnsub()
}

// this is a basic implementation. See `RemoteUserNatWindowedClient` for a more robust implementation
type RemoteUserNatClient struct {
	client                *Client
	receivePacketCallback ReceivePacketFunction
	securityPolicy        *SecurityPolicy
	pathTable             *pathTable
	// the provide mode of the source packets
	// for locally generated packets this is `ProvideMode_Network`
	provideMode protocol.ProvideMode
	clientUnsub func()
}

func NewRemoteUserNatClient(
	client *Client,
	receivePacketCallback ReceivePacketFunction,
	destinations []MultiHopId,
	provideMode protocol.ProvideMode,
) (*RemoteUserNatClient, error) {
	pathTable, err := newPathTable(destinations)
	if err != nil {
		return nil, err
	}

	userNatClient := &RemoteUserNatClient{
		client:                client,
		receivePacketCallback: receivePacketCallback,
		securityPolicy:        DefaultSecurityPolicy(),
		pathTable:             pathTable,
		provideMode:           provideMode,
	}

	clientUnsub := client.AddReceiveCallback(userNatClient.ClientReceive)
	userNatClient.clientUnsub = clientUnsub

	return userNatClient, nil
}

// `SendPacketFunction`
func (self *RemoteUserNatClient) SendPacket(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
	minRelationship := max(provideMode, self.provideMode)

	ipPath, r, err := self.securityPolicy.Inspect(minRelationship, packet)
	if err != nil {
		return false
	}
	switch r {
	case SecurityPolicyResultAllow:
		destination, err := self.pathTable.SelectDestination(packet)
		if err != nil {
			// drop
			return false
		}

		ipPacketToProvider := &protocol.IpPacketToProvider{
			IpPacket: &protocol.IpPacket{
				PacketBytes: packet,
			},
		}
		frame, err := ToFrame(ipPacketToProvider)
		if err != nil {
			panic(err)
		}

		// the sender will control transfer
		opts := []any{}
		switch ipPath.Protocol {
		case IpProtocolUdp:
			opts = append(opts, NoAck())
		}
		success := self.client.SendMultiHopWithTimeout(frame, destination, func(err error) {}, timeout, opts...)
		return success
	default:
		return false
	}
}

// `connect.ReceiveFunction`
func (self *RemoteUserNatClient) ClientReceive(source TransferPath, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
	// only process frames from the destinations
	// if allow := self.sourceFilter[source]; !allow {
	//     return
	// }

	for _, frame := range frames {
		switch frame.MessageType {
		case protocol.MessageType_IpIpPacketFromProvider:
			ipPacketFromProvider_, err := FromFrame(frame)
			if err != nil {
				panic(err)
			}
			ipPacketFromProvider := ipPacketFromProvider_.(*protocol.IpPacketFromProvider)

			HandleError(func() {
				self.receivePacketCallback(source, IpProtocolUnknown, ipPacketFromProvider.IpPacket.PacketBytes)
			})
		}
	}
}

func (self *RemoteUserNatClient) Shuffle() {
}

func (self *RemoteUserNatClient) Close() {
	// self.client.RemoveReceiveCallback(self.clientCallbackId)
	self.clientUnsub()
}

type pathTable struct {
	destinations []MultiHopId

	// TODO clean up entries that haven't been used in some time
	paths4 map[Ip4Path]MultiHopId
	paths6 map[Ip6Path]MultiHopId
}

func newPathTable(destinations []MultiHopId) (*pathTable, error) {
	if len(destinations) == 0 {
		return nil, errors.New("No destinations.")
	}
	return &pathTable{
		destinations: destinations,
		paths4:       map[Ip4Path]MultiHopId{},
		paths6:       map[Ip6Path]MultiHopId{},
	}, nil
}

func (self *pathTable) SelectDestination(packet []byte) (MultiHopId, error) {
	if len(self.destinations) == 1 {
		return self.destinations[0], nil
	}

	ipPath, err := ParseIpPath(packet)
	if err != nil {
		return MultiHopId{}, err
	}
	switch ipPath.Version {
	case 4:
		ip4Path := ipPath.ToIp4Path()
		if destination, ok := self.paths4[ip4Path]; ok {
			return destination, nil
		}
		i := mathrand.Intn(len(self.destinations))
		destination := self.destinations[i]
		self.paths4[ip4Path] = destination
		return destination, nil
	case 6:
		ip6Path := ipPath.ToIp6Path()
		if destination, ok := self.paths6[ip6Path]; ok {
			return destination, nil
		}
		i := mathrand.Intn(len(self.destinations))
		destination := self.destinations[i]
		self.paths6[ip6Path] = destination
		return destination, nil
	default:
		// no support for this version
		return MultiHopId{}, fmt.Errorf("No support for ip version %d", ipPath.Version)
	}
}

type IpProtocol int

const (
	IpProtocolUnknown IpProtocol = 0
	IpProtocolTcp     IpProtocol = 1
	IpProtocolUdp     IpProtocol = 2
)

type IpPath struct {
	Version         int
	Protocol        IpProtocol
	SourceIp        net.IP
	SourcePort      int
	DestinationIp   net.IP
	DestinationPort int
}

func ParseIpPath(ipPacket []byte) (*IpPath, error) {
	ipVersion := uint8(ipPacket[0]) >> 4
	switch ipVersion {
	case 4:
		ipv4 := layers.IPv4{}
		ipv4.DecodeFromBytes(ipPacket, gopacket.NilDecodeFeedback)
		switch ipv4.Protocol {
		case layers.IPProtocolUDP:
			udp := layers.UDP{}
			udp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

			return &IpPath{
				Version:         int(ipVersion),
				Protocol:        IpProtocolUdp,
				SourceIp:        ipv4.SrcIP,
				SourcePort:      int(udp.SrcPort),
				DestinationIp:   ipv4.DstIP,
				DestinationPort: int(udp.DstPort),
			}, nil
		case layers.IPProtocolTCP:
			tcp := layers.TCP{}
			tcp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

			return &IpPath{
				Version:         int(ipVersion),
				Protocol:        IpProtocolTcp,
				SourceIp:        ipv4.SrcIP,
				SourcePort:      int(tcp.SrcPort),
				DestinationIp:   ipv4.DstIP,
				DestinationPort: int(tcp.DstPort),
			}, nil
		default:
			// no support for this protocol
			return nil, fmt.Errorf("No support for protocol %d", ipv4.Protocol)
		}
	case 6:
		ipv6 := layers.IPv6{}
		ipv6.DecodeFromBytes(ipPacket, gopacket.NilDecodeFeedback)
		switch ipv6.NextHeader {
		case layers.IPProtocolUDP:
			udp := layers.UDP{}
			udp.DecodeFromBytes(ipv6.Payload, gopacket.NilDecodeFeedback)

			return &IpPath{
				Version:         int(ipVersion),
				Protocol:        IpProtocolUdp,
				SourceIp:        ipv6.SrcIP,
				SourcePort:      int(udp.SrcPort),
				DestinationIp:   ipv6.DstIP,
				DestinationPort: int(udp.DstPort),
			}, nil
		case layers.IPProtocolTCP:
			tcp := layers.TCP{}
			tcp.DecodeFromBytes(ipv6.Payload, gopacket.NilDecodeFeedback)

			return &IpPath{
				Version:         int(ipVersion),
				Protocol:        IpProtocolTcp,
				SourceIp:        ipv6.SrcIP,
				SourcePort:      int(tcp.SrcPort),
				DestinationIp:   ipv6.DstIP,
				DestinationPort: int(tcp.DstPort),
			}, nil
		default:
			// no support for this protocol
			return nil, fmt.Errorf("No support for protocol %d", ipv6.NextHeader)
		}
	default:
		// no support for this version
		return nil, fmt.Errorf("No support for ip version %d", ipVersion)
	}
}

func (self *IpPath) ToIp4Path() Ip4Path {
	var sourceIp [4]byte
	if self.SourceIp != nil {
		if sourceIp4 := self.SourceIp.To4(); sourceIp4 != nil {
			sourceIp = [4]byte(sourceIp4)
		}
	}
	var destinationIp [4]byte
	if self.DestinationIp != nil {
		if destinationIp4 := self.DestinationIp.To4(); destinationIp4 != nil {
			destinationIp = [4]byte(destinationIp4)
		}
	}
	return Ip4Path{
		Protocol:        self.Protocol,
		SourceIp:        sourceIp,
		SourcePort:      self.SourcePort,
		DestinationIp:   destinationIp,
		DestinationPort: self.DestinationPort,
	}
}

func (self *IpPath) ToIp6Path() Ip6Path {
	var sourceIp [16]byte
	if self.SourceIp != nil {
		if sourceIp6 := self.SourceIp.To16(); sourceIp6 != nil {
			sourceIp = [16]byte(sourceIp6)
		}
	}
	var destinationIp [16]byte
	if self.DestinationIp != nil {
		if destinationIp6 := self.DestinationIp.To16(); destinationIp6 != nil {
			destinationIp = [16]byte(destinationIp6)
		}
	}
	return Ip6Path{
		Protocol:        self.Protocol,
		SourceIp:        sourceIp,
		SourcePort:      self.SourcePort,
		DestinationIp:   destinationIp,
		DestinationPort: self.DestinationPort,
	}
}

func (self *IpPath) Source() *IpPath {
	return &IpPath{
		Protocol:   self.Protocol,
		Version:    self.Version,
		SourceIp:   self.SourceIp,
		SourcePort: self.SourcePort,
	}
}

func (self *IpPath) Destination() *IpPath {
	return &IpPath{
		Protocol:        self.Protocol,
		Version:         self.Version,
		DestinationIp:   self.DestinationIp,
		DestinationPort: self.DestinationPort,
	}
}

// comparable
type Ip4Path struct {
	Protocol        IpProtocol
	SourceIp        [4]byte
	SourcePort      int
	DestinationIp   [4]byte
	DestinationPort int
}

func (self *Ip4Path) Source() Ip4Path {
	return Ip4Path{
		Protocol:   self.Protocol,
		SourceIp:   self.SourceIp,
		SourcePort: self.SourcePort,
	}
}

func (self *Ip4Path) Destination() Ip4Path {
	return Ip4Path{
		Protocol:        self.Protocol,
		DestinationIp:   self.DestinationIp,
		DestinationPort: self.DestinationPort,
	}
}

// comparable
type Ip6Path struct {
	Protocol        IpProtocol
	SourceIp        [16]byte
	SourcePort      int
	DestinationIp   [16]byte
	DestinationPort int
}

func (self *Ip6Path) Source() Ip6Path {
	return Ip6Path{
		Protocol:   self.Protocol,
		SourceIp:   self.SourceIp,
		SourcePort: self.SourcePort,
	}
}

func (self *Ip6Path) Destination() Ip6Path {
	return Ip6Path{
		Protocol:        self.Protocol,
		DestinationIp:   self.DestinationIp,
		DestinationPort: self.DestinationPort,
	}
}

type SecurityPolicyResult int

const (
	SecurityPolicyResultDrop     SecurityPolicyResult = 0
	SecurityPolicyResultAllow    SecurityPolicyResult = 1
	SecurityPolicyResultIncident SecurityPolicyResult = 2
)

type SecurityPolicy struct {
}

func DefaultSecurityPolicy() *SecurityPolicy {
	return &SecurityPolicy{}
}

func (self *SecurityPolicy) Inspect(provideMode protocol.ProvideMode, packet []byte) (*IpPath, SecurityPolicyResult, error) {
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		// bad ip packet
		return ipPath, SecurityPolicyResultDrop, err
	}

	if protocol.ProvideMode_Public <= provideMode {
		// apply public rules:
		// - only public unicast network destinations
		// - block insecure or known unencrypted traffic

		if !isPublicUnicast(ipPath.DestinationIp) {
			return ipPath, SecurityPolicyResultIncident, nil
		}

		// block insecure or unencrypted traffic
		// block known destructive protocols
		// - allow secure web and dns traffic (443)
		// - allow email protocols (465, 993, 995)
		// - allow dns over tls (853)
		// - allow user ports (>=1024)
		// - block bittorrent (6881-6889)
		// - FIXME temporarily enabling 53 and 80 until inline protocol translation is implemented
		// TODO in the future, allow a control message to dynamically adjust the security rules
		allow := func() bool {
			switch port := ipPath.DestinationPort; {
			case port == 53:
				// dns
				// FIXME for now we allow plain dns. TODO to upgrade the protocol to doh inline.
				return true
			case port == 80:
				// http
				// FIXME for now we allow http. It's important for some radio streaming. TODO to upgrade the protcol to https inline.
				return true
			case port == 443:
				// https
				return true
			case port == 465, port == 993, port == 995:
				// email
				return true
			case port == 853:
				// dns over tls
				return true
			case port < 1024:
				return false
			case 6881 <= port && port <= 6889:
				// bittorrent
				return false
			default:
				return true
			}
		}
		if !allow() {
			return ipPath, SecurityPolicyResultDrop, nil
		}
	}

	return ipPath, SecurityPolicyResultAllow, nil
}

func isPublicUnicast(ip net.IP) bool {
	switch {
	case ip.IsPrivate(),
		ip.IsLoopback(),
		ip.IsLinkLocalUnicast(),
		ip.IsMulticast(),
		ip.IsUnspecified():
		return false
	default:
		return true
	}
}

type UserLimited interface {
	LastActivityTime() time.Time
	Cancel()
}

type userLimited struct {
	mutex            sync.Mutex
	lastActivityTime time.Time
}

func newUserLimited() *userLimited {
	return &userLimited{
		lastActivityTime: time.Now(),
	}
}

func (self *userLimited) LastActivityTime() time.Time {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.lastActivityTime
}

func (self *userLimited) UpdateLastActivityTime() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.lastActivityTime = time.Now()
}

func applyLruUserLimit[R UserLimited](resources []R, ulimit int, limitCallback func(R) bool) {
	// limit the total connections per source to avoid blowing up the ulimit
	if n := len(resources) - ulimit; 0 < n {
		resourceLastActivityTimes := map[UserLimited]time.Time{}
		for _, resource := range resources {
			resourceLastActivityTimes[resource] = resource.LastActivityTime()
		}
		// order by last activity time
		slices.SortFunc(resources, func(a R, b R) int {
			lastActivityTimeA := resourceLastActivityTimes[a]
			lastActivityTimeB := resourceLastActivityTimes[b]
			if lastActivityTimeA.Before(lastActivityTimeB) {
				return -1
			} else if lastActivityTimeB.Before(lastActivityTimeA) {
				return 1
			} else {
				return 0
			}
		})
		i := 0
		for _, resource := range resources {
			if limitCallback(resource) {
				i += 1
				resource.Cancel()
			}
			if n <= i {
				break
			}
		}
	}
}

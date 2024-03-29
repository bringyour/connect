package connect

import (
    "net"
    "context"
    "time"
    "sync"
    "strconv"
    "fmt"
    "strings"
    "errors"
    mathrand "math/rand"
    "math"
    "io"

    "github.com/google/gopacket"
    "github.com/google/gopacket/layers"

    // "google.golang.org/protobuf/proto"

    "bringyour.com/protocol"
)


// implements user-space NAT (UNAT) and packet inspection
// The UNAT emulates a raw socket using user-space sockets.


var ipLog = LogFn(LogLevelDebug, "ip")

const DefaultMtu = 1440
const Ipv4HeaderSizeWithoutExtensions = 20
const Ipv6HeaderSize = 40
const UdpHeaderSize = 8
const TcpHeaderSizeWithoutExtensions = 20

const debugVerifyHeaders = false


// send from a raw socket
// note `ipProtocol` is not supplied. The implementation must do a packet inspection to determine protocol
type SendPacketFunction func(source Path, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool


// receive into a raw socket
type ReceivePacketFunction func(source Path, ipProtocol IpProtocol, packet []byte)


type UserNatClient interface {
    // `SendPacketFunction`
    SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool
    Close()
}


func DefaultUdpBufferSettings() *UdpBufferSettings {
    return &UdpBufferSettings{
        ReadTimeout: 30 * time.Second,
        WriteTimeout: 30 * time.Second,
        IdleTimeout: 60 * time.Second,
        Mtu: DefaultMtu,
        // avoid fragmentation
        ReadBufferSize: DefaultMtu - max(Ipv4HeaderSizeWithoutExtensions, Ipv6HeaderSize) - max(UdpHeaderSize, TcpHeaderSizeWithoutExtensions),
        ChannelBufferSize: 32,
    }
}


func DefaultTcpBufferSettings() *TcpBufferSettings {
    tcpBufferSettings := &TcpBufferSettings{
        ConnectTimeout: 60 * time.Second,
        ReadTimeout: 30 * time.Second,
        WriteTimeout: 30 * time.Second,
        IdleTimeout: 60 * time.Second,
        ChannelBufferSize: 32,
        Mtu: DefaultMtu,
        // avoid fragmentation
        ReadBufferSize: DefaultMtu - max(Ipv4HeaderSizeWithoutExtensions, Ipv6HeaderSize) - max(UdpHeaderSize, TcpHeaderSizeWithoutExtensions),
        WindowSize: int(mib(1)),
    }
    return tcpBufferSettings
}


func DefaultLocalUserNatSettings() *LocalUserNatSettings {
    return &LocalUserNatSettings{
        SendBufferSize: 32,
        BufferTimeout: 5 * time.Second,
        UdpBufferSettings: DefaultUdpBufferSettings(),
        TcpBufferSettings: DefaultTcpBufferSettings(),
    }
}


type LocalUserNatSettings struct {
    SendBufferSize int
    BufferTimeout time.Duration
    UdpBufferSettings *UdpBufferSettings
    TcpBufferSettings *TcpBufferSettings
}


// forwards packets using user space sockets
// this assumes transfer between the packet source and this is lossless and in order,
// so the protocol stack implementations do not implement any retransmit logic
type LocalUserNat struct {
    ctx context.Context
    cancel context.CancelFunc
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
        ctx: cancelCtx,
        cancel: cancel,
        clientTag: clientTag,
        sendPackets: make(chan *SendPacket, settings.SendBufferSize),
        settings: settings,
        receiveCallbacks: NewCallbackList[ReceivePacketFunction](),
    }
    go localUserNat.Run()

    return localUserNat
}

// TODO provide mode of the destination determines filtering rules - e.g. local networks
// TODO currently filter all local networks and non-encrypted traffic
func (self *LocalUserNat) SendPacketWithTimeout(source Path, provideMode protocol.ProvideMode,
        packet []byte, timeout time.Duration) bool {
    ipLog("SendWithTimeout(%s, %s, %d, %s)", source, provideMode, len(packet), timeout)
    sendPacket := &SendPacket{
        source: source,
        provideMode: provideMode,
        packet: packet,
    }
    if timeout < 0 {
        select {
        case <- self.ctx.Done():
            return false
        case self.sendPackets <- sendPacket:
            return true
        }
    } else if 0 == timeout {
        select {
        case <- self.ctx.Done():
            return false
        case self.sendPackets <- sendPacket:
            return true
        default:
            // full
            return false
        }
    } else {
        select {
        case <- self.ctx.Done():
            return false
        case self.sendPackets <- sendPacket:
            return true
        case <- time.After(timeout):
            // full
            return false
        }
    }
}

// `SendPacketFunction`
func (self *LocalUserNat) SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
    return self.SendPacketWithTimeout(source, provideMode, packet, timeout)
}

// func (self *LocalUserNat) ReceiveN(source Path, provideMode protocol.ProvideMode, packet []byte, n int) {
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
func (self *LocalUserNat) receive(source Path, ipProtocol IpProtocol, packet []byte) {
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
        case <- self.ctx.Done():
            return
        case sendPacket := <- self.sendPackets:
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

                    TraceWithReturn(
                        fmt.Sprintf("[unpr]send udp4 %s<-%s", self.clientTag, sendPacket.source.ClientId.String()),
                        func()(bool) {
                            success, err := udp4Buffer.send(
                                sendPacket.source,
                                sendPacket.provideMode,
                                &ipv4,
                                &udp,
                                self.settings.BufferTimeout,
                            )
                            return success && err == nil
                        },
                    )
                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

                    TraceWithReturn(
                        fmt.Sprintf("[unpr]send tcp4 %s<-%s", self.clientTag, sendPacket.source.ClientId.String()),
                        func()(bool) {
                            success, err := tcp4Buffer.send(
                                sendPacket.source,
                                sendPacket.provideMode,
                                &ipv4,
                                &tcp,
                                self.settings.BufferTimeout,
                            )
                            return success && err == nil
                        },
                    )
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

                    TraceWithReturn(
                        fmt.Sprintf("[unpr]send udp6 %s<-%s", self.clientTag, sendPacket.source.ClientId.String()),
                        func()(bool) {
                            success, err := udp6Buffer.send(
                                sendPacket.source,
                                sendPacket.provideMode,
                                &ipv6,
                                &udp,
                                self.settings.BufferTimeout,
                            )
                            return success && err == nil
                        },
                    )
                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv6.Payload, gopacket.NilDecodeFeedback)

                    TraceWithReturn(
                        fmt.Sprintf("[unpr]send tcp6 %s<-%s", self.clientTag, sendPacket.source.ClientId.String()),
                        func()(bool) {
                            success, err := tcp6Buffer.send(
                                sendPacket.source,
                                sendPacket.provideMode,
                                &ipv6,
                                &tcp,
                                self.settings.BufferTimeout,
                            )
                            return success && err == nil
                        },
                    )
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
    source Path
    provideMode protocol.ProvideMode
    packet []byte
}


// comparable
type BufferId4 struct {
    source Path
    sourceIp [4]byte
    sourcePort int
    destinationIp [4]byte
    destinationPort int
}

func NewBufferId4(source Path, sourceIp net.IP, sourcePort int, destinationIp net.IP, destinationPort int) BufferId4 {
    return BufferId4{
        source: source,
        sourceIp: [4]byte(sourceIp),
        sourcePort: sourcePort,
        destinationIp: [4]byte(destinationIp),
        destinationPort: destinationPort,
    }
}


// comparable
type BufferId6 struct {
    source Path
    sourceIp [16]byte
    sourcePort int
    destinationIp [16]byte
    destinationPort int
}

func NewBufferId6(source Path, sourceIp net.IP, sourcePort int, destinationIp net.IP, destinationPort int) BufferId6 {
    return BufferId6{
        source: source,
        sourceIp: [16]byte(sourceIp),
        sourcePort: sourcePort,
        destinationIp: [16]byte(destinationIp),
        destinationPort: destinationPort,
    }
}


type UdpBufferSettings struct {
    ReadTimeout time.Duration
    WriteTimeout time.Duration
    IdleTimeout time.Duration
    Mtu int
    ReadBufferSize int
    ChannelBufferSize int
}


type Udp4Buffer struct {
    UdpBuffer[BufferId4]
}

func NewUdp4Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
        udpBufferSettings *UdpBufferSettings) *Udp4Buffer {
    return &Udp4Buffer{
        UdpBuffer: UdpBuffer[BufferId4]{
            ctx: ctx,
            receiveCallback: receiveCallback,
            udpBufferSettings: udpBufferSettings,
            sequences: map[BufferId4]*UdpSequence{},
        },
    }
}

func (self *Udp4Buffer) send(source Path, provideMode protocol.ProvideMode,
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
        UdpBuffer: UdpBuffer[BufferId6]{
            ctx: ctx,
            receiveCallback: receiveCallback,
            udpBufferSettings: udpBufferSettings,
            sequences: map[BufferId6]*UdpSequence{},
        },
    }
}

func (self *Udp6Buffer) send(source Path, provideMode protocol.ProvideMode,
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
    ctx context.Context
    receiveCallback ReceivePacketFunction
    udpBufferSettings *UdpBufferSettings

    mutex sync.Mutex

    sequences map[BufferId]*UdpSequence
}


func (self *UdpBuffer[BufferId]) udpSend(
    bufferId BufferId,
    sourceIp net.IP,
    destinationIp net.IP,
    source Path,
    provideMode protocol.ProvideMode,
    ipVersion int,
    udp *layers.UDP,
    timeout time.Duration,
) (bool, error) {
    initSequence := func(skip *UdpSequence)(*UdpSequence) {
        self.mutex.Lock()
        defer self.mutex.Unlock()

        sequence, ok := self.sequences[bufferId]
        if ok {
            if skip == nil || skip != sequence {
                return sequence
            } else {
                sequence.Close()
                delete(self.sequences, bufferId)
            }
        }
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
            // clean up
            if sequence == self.sequences[bufferId] {
                delete(self.sequences, bufferId)
            }
        }()
        return sequence
    }

    sendItem := &UdpSendItem{
        provideMode: provideMode,
        udp: udp,
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
    log LogFunction
    ctx context.Context
    cancel context.CancelFunc
    receiveCallback ReceivePacketFunction
    udpBufferSettings *UdpBufferSettings

    sendItems chan *UdpSendItem

    idleCondition *IdleCondition

    StreamState
}

func NewUdpSequence(ctx context.Context, receiveCallback ReceivePacketFunction,
        source Path, 
        ipVersion int,
        sourceIp net.IP, sourcePort layers.UDPPort,
        destinationIp net.IP, destinationPort layers.UDPPort,
        udpBufferSettings *UdpBufferSettings) *UdpSequence {
    cancelCtx, cancel := context.WithCancel(ctx)
    streamState := StreamState{
        source: source,
        ipVersion: ipVersion,
        sourceIp: sourceIp,
        sourcePort: sourcePort,
        destinationIp: destinationIp,
        destinationPort: destinationPort,
    }
    return &UdpSequence{
        log: SubLogFn(LogLevelDebug, ipLog, fmt.Sprintf(
            "UdpSequence(%s %s)",
            streamState.SourceAuthority(),
            streamState.DestinationAuthority(),
        )),
        ctx: cancelCtx,
        cancel: cancel,
        receiveCallback: receiveCallback,
        sendItems: make(chan *UdpSendItem, udpBufferSettings.ChannelBufferSize),
        udpBufferSettings: udpBufferSettings,
        idleCondition: NewIdleCondition(),
        StreamState: streamState,
    }
}

func (self *UdpSequence) send(sendItem *UdpSendItem, timeout time.Duration) (bool, error) {
    if !self.idleCondition.UpdateOpen() {
        return false, nil
    }
    defer self.idleCondition.UpdateClose()

    select {
    case <- self.ctx.Done():
        return false, errors.New("Done.")
    default:
    }

    if timeout < 0 {
        select {
        case <- self.ctx.Done():
            return false, errors.New("Done.")
        case self.sendItems <- sendItem:
            return true, nil
        }
    } else if timeout == 0 {
        select {
        case <- self.ctx.Done():
            return false, errors.New("Done.")
        case self.sendItems <- sendItem:
            return true, nil
        default:
            return false, nil
        }
    } else {
        select {
        case <- self.ctx.Done():
            return false, errors.New("Done.")
        case self.sendItems <- sendItem:
            return true, nil
        case <- time.After(timeout):
            return false, nil
        }
    }
}

func (self *UdpSequence) Run() {
    defer func() {
        self.cancel()

        // close(self.sendItems)
        // drain and drop
        func() {
            for {
                select {
                case _, ok := <- self.sendItems:
                    if !ok {
                        return
                    }
                }
            }
        }()
    }()

    receive := func(packet []byte) {
        self.receiveCallback(self.source, IpProtocolUdp, packet)
    }

    self.log("[init]connect")
    socket, err := net.Dial(
        "udp",
        self.DestinationAuthority(),
    )
    if err != nil {
        return
    }
    defer socket.Close()
    self.log("[init]connect success")

    go func() {
        defer self.cancel()

        buffer := make([]byte, self.udpBufferSettings.ReadBufferSize)

        readTimeout := time.Now().Add(self.udpBufferSettings.ReadTimeout)
        for forwardIter := 0; ; forwardIter += 1 {
            select {
            case <- self.ctx.Done():
                return
            default:
            }

            socket.SetReadDeadline(readTimeout)
            n, err := socket.Read(buffer)

            if err == nil {
                self.log("[f%d]send(%d)", forwardIter, n)
            } else {
                fmt.Printf("[f%d]send(%d err=%s)\n", forwardIter, n, err)
            }

            if 0 < n {
                packets, err := self.DataPackets(buffer, n, self.udpBufferSettings.Mtu)
                if err != nil {
                    return
                }
                if 1 < len(packets) {
                    fmt.Printf("SEGMENTED PACKETS (%d)\n", len(packets))
                }
                for _, packet := range packets {
                    self.log("[f%d]receive (%d)", forwardIter, len(packet))
                    receive(packet)
                }
                readTimeout = time.Now().Add(self.udpBufferSettings.ReadTimeout)
            }

            if err != nil {
                if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                    if readTimeout.Before(time.Now()) {
                        self.log("[f%d]timeout")
                        return
                    }
                } else {
                    // some other error
                    return
                }
            }
        }
    }()

    for sendIter := 0; ; sendIter += 1 {
        checkpointId := self.idleCondition.Checkpoint()
        select {
        case <- self.ctx.Done():
            return
        case sendItem := <- self.sendItems:
            writeTimeout := time.Now().Add(self.udpBufferSettings.WriteTimeout)

            payload := sendItem.udp.Payload

            self.log("[r%d]receive(%d)", sendIter, len(payload))

            for 0 < len(payload) {
                select {
                case <- self.ctx.Done():
                    return
                default:
                }

                socket.SetWriteDeadline(writeTimeout)
                n, err := socket.Write(payload)

                if err == nil {
                    self.log("[r%d]forward (%d)", sendIter, n)
                } else {
                    self.log("[r%d]forward (%d err=%s)", sendIter, n, err)
                }

                payload = payload[n:]

                if err != nil {
                    if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                        if writeTimeout.Before(time.Now()) {
                            return
                        }
                    } else {
                        // some other error
                        return
                    }
                }
            }
        case <- time.After(self.udpBufferSettings.IdleTimeout):
            if self.idleCondition.Close(checkpointId) {
                // close the sequence
                self.log("[r%d]timeout", sendIter)
                return
            }
            // else there pending updates
        }
    }
}

func (self *UdpSequence) Close() {
    self.cancel()
}

type UdpSendItem struct {
    source Path
    provideMode protocol.ProvideMode
    udp *layers.UDP
}

type StreamState struct {
    source Path
    ipVersion int
    sourceIp net.IP
    sourcePort layers.UDPPort
    destinationIp net.IP
    destinationPort layers.UDPPort
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
            Version: 4,
            TTL: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            Protocol: layers.IPProtocolUDP,
        }
        headerSize += Ipv4HeaderSizeWithoutExtensions
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
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
        FixLengths: true,
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


    if headerSize + n <= mtu {
        buffer := gopacket.NewSerializeBufferExpectedSize(headerSize + n, 0)
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
        packets := make([][]byte, 0, (n + packetSize) / packetSize)
        for i := 0; i < n; {
            j := min(i + packetSize, n)
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
    ConnectTimeout time.Duration
    ReadTimeout time.Duration
    WriteTimeout time.Duration
    // ReadPollTimeout time.Duration
    // WritePollTimeout time.Duration
    IdleTimeout time.Duration
    ReadBufferSize int
    ChannelBufferSize int
    Mtu int
    // the window size is the max amount of packet data in memory for each sequence
    // TODO currently we do not enable window scale
    // TODO this value is max 2^16
    WindowSize int
}


type Tcp4Buffer struct {
    TcpBuffer[BufferId4]
}

func NewTcp4Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
        tcpBufferSettings *TcpBufferSettings) *Tcp4Buffer {
    return &Tcp4Buffer{
        TcpBuffer: TcpBuffer[BufferId4]{
            ctx: ctx,
            receiveCallback: receiveCallback,
            tcpBufferSettings: tcpBufferSettings,
            sequences: map[BufferId4]*TcpSequence{},
        },
    }
}

func (self *Tcp4Buffer) send(source Path, provideMode protocol.ProvideMode, 
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

func (self *TcpSequence) Close() {
    self.cancel()
}


type Tcp6Buffer struct {
    TcpBuffer[BufferId6]
}

func NewTcp6Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
        tcpBufferSettings *TcpBufferSettings) *Tcp6Buffer {
    return &Tcp6Buffer{
        TcpBuffer: TcpBuffer[BufferId6]{
            ctx: ctx,
            receiveCallback: receiveCallback,
            tcpBufferSettings: tcpBufferSettings,
            sequences: map[BufferId6]*TcpSequence{},
        },
    }
}

func (self *Tcp6Buffer) send(source Path, provideMode protocol.ProvideMode, 
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
    ctx context.Context
    receiveCallback ReceivePacketFunction
    tcpBufferSettings *TcpBufferSettings

    mutex sync.Mutex

    sequences map[BufferId]*TcpSequence
}


func (self *TcpBuffer[BufferId]) tcpSend(
    bufferId BufferId,
    sourceIp net.IP,
    destinationIp net.IP,
    source Path,
    provideMode protocol.ProvideMode,
    ipVersion int,
    tcp *layers.TCP,
    timeout time.Duration,
) (bool, error) {
    initSequence := func()(*TcpSequence) {
        self.mutex.Lock()
        defer self.mutex.Unlock()

        // new sequence
        if tcp.SYN {
            if sequence, ok := self.sequences[bufferId]; ok {
                sequence.Close()
                delete(self.sequences, bufferId)
            }
        }

        if sequence, ok := self.sequences[bufferId]; ok {
            return sequence
        }

        if tcp.SYN {
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
                // clean up
                if sequence == self.sequences[bufferId] {
                    delete(self.sequences, bufferId)
                }
            }()
            return sequence
        } else {
            // drop the packet; only create a new sequence on SYN
            ipLog("TcpBuffer drop (%s %s %s)",
                net.JoinHostPort(
                    sourceIp.String(),
                    strconv.Itoa(int(tcp.SrcPort)),
                ),
                net.JoinHostPort(
                    destinationIp.String(),
                    strconv.Itoa(int(tcp.DstPort)),
                ),
                tcpFlagsString(tcp),
            )
            return nil
        }
    }
    sendItem := &TcpSendItem{
        provideMode: provideMode,
        tcp: tcp,
    }
    if sequence := initSequence(); sequence == nil {
        // sequence does not exist and not a syn packet, drop
        fmt.Printf("[unpr]tcp4 drop no syn (%s)\n", tcpFlagsString(tcp))
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
    log LogFunction
    ctx context.Context
    cancel context.CancelFunc
    
    receiveCallback ReceivePacketFunction

    tcpBufferSettings *TcpBufferSettings

    sendItems chan *TcpSendItem

    idleCondition *IdleCondition

    ConnectionState
}

func NewTcpSequence(ctx context.Context, receiveCallback ReceivePacketFunction,
        source Path,
        ipVersion int,
        sourceIp net.IP, sourcePort layers.TCPPort,
        destinationIp net.IP, destinationPort layers.TCPPort,
        tcpBufferSettings *TcpBufferSettings) *TcpSequence {
    cancelCtx, cancel := context.WithCancel(ctx)

    var windowSize uint16
    if math.MaxUint16 < tcpBufferSettings.WindowSize {
        windowSize = math.MaxUint16
    } else {
        windowSize = uint16(tcpBufferSettings.WindowSize)
    }

    connectionState := ConnectionState{
        source: source,
        ipVersion: ipVersion,
        sourceIp: sourceIp,
        sourcePort: sourcePort,
        destinationIp: destinationIp,
        destinationPort: destinationPort,
        // the window size starts at the fixed value
        windowSize: windowSize,
    }
    return &TcpSequence{
        log: SubLogFn(LogLevelDebug, ipLog, fmt.Sprintf(
            "TcpSequence(%s %s)",
            connectionState.SourceAuthority(),
            connectionState.DestinationAuthority(),
        )),
        ctx: cancelCtx,
        cancel: cancel,
        receiveCallback: receiveCallback,
        tcpBufferSettings: tcpBufferSettings,
        sendItems: make(chan *TcpSendItem, tcpBufferSettings.ChannelBufferSize),
        idleCondition: NewIdleCondition(),
        ConnectionState: connectionState,
    }
}

func (self *TcpSequence) send(sendItem *TcpSendItem, timeout time.Duration) (bool, error) {
    if !self.idleCondition.UpdateOpen() {
        return false, nil
    }
    defer self.idleCondition.UpdateClose()

    select {
    case <- self.ctx.Done():
        return false, errors.New("Done.")
    default:
    }

    if timeout < 0 {
        select {
        case <- self.ctx.Done():
            return false, errors.New("Done.")
        case self.sendItems <- sendItem:
            return true, nil
        }
    } else if timeout == 0 {
        select {
        case <- self.ctx.Done():
            return false, errors.New("Done.")
        case self.sendItems <- sendItem:
            return true, nil
        default:
            return false, nil
        }
    } else {
        select {
        case <- self.ctx.Done():
            return false, errors.New("Done.")
        case self.sendItems <- sendItem:
            return true, nil
        case <- time.After(timeout):
            return false, nil
        }
    }
}

func (self *TcpSequence) Run() {
    defer func() {
        self.cancel()

        // close(self.sendItems)

        // drain and drop
        func() {
            for {
                select {
                case _, ok := <- self.sendItems:
                    if !ok {
                        return
                    }
                }
            }
        }()
    }()


    receive := func(packet []byte) {
        self.receiveCallback(self.source, IpProtocolTcp, packet)
    }

    closed := false
    // send a final FIN+ACK
    defer func() {
        if closed {
            fmt.Printf("[r]closed gracefully\n")
        } else {
            fmt.Printf("[r]closed unexpected sending RST\n")
            self.log("[final]RST")
            var packet []byte
            var err error
            func() {
                self.mutex.Lock()
                defer self.mutex.Unlock()

                packet, err = self.RstAck()
            }()
            if err == nil {
                self.log("[final]send RST (%d)", self.sendSeq)
                receive(packet)
            }
        }
    }()


    for syn := false; !syn; {
        select {
        case <- self.ctx.Done():
            return
        case sendItem := <- self.sendItems:
            self.log("[init]send(%d)", len(sendItem.tcp.BaseLayer.Payload))
            // the first packet must be a syn
            if sendItem.tcp.SYN {
                self.log("[init]SYN")

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
                    self.receiveWindowSize = sendItem.tcp.Window
                    packet, err = self.SynAck()
                    self.receiveSeq += 1
                }()
                if err == nil {
                    self.log("[init]receive SYN+ACK")
                    receive(packet)
                }
                
                syn = true
            } else {
                // an ACK here could be for a previous FIN
                self.log("[init]waiting for SYN (%s)", tcpFlagsString(sendItem.tcp))
            }
        }
    }

    self.log("[init]connect")
    socket, err := net.DialTimeout(
        "tcp",
        self.DestinationAuthority(),
        self.tcpBufferSettings.ConnectTimeout,
    )
    if err != nil {
        self.log("[init]connect error (%s)", err)
        return
    }
    defer socket.Close()

    tcpSocket := socket.(*net.TCPConn)
    tcpSocket.SetNoDelay(false)

    self.log("[init]connect success")

    receiveAckCond := sync.NewCond(&self.mutex)
    defer func() {
        self.mutex.Lock()
        defer self.mutex.Unlock()

        receiveAckCond.Broadcast()
    }()

    go func() {
        defer self.cancel()

        buffer := make([]byte, self.tcpBufferSettings.ReadBufferSize)
        
        readTimeout := time.Now().Add(self.tcpBufferSettings.ReadTimeout)
        for forwardIter := 0; ; forwardIter += 1 {
            select {
            case <- self.ctx.Done():
                return
            default:
            }

            socket.SetReadDeadline(readTimeout)
            
            n, err := socket.Read(buffer)

            if err == nil {
                self.log("[f%d]send(%d)", forwardIter, n)
            } else {
                fmt.Printf("[f%d]send(%d) (err=%s)\n", forwardIter, n, err)
            }

            if 0 < n {
                // since the transfer from local to remove is lossless and preserves order,
                // do not worry about retransmits
                var packets [][]byte
                var packetsErr error
                func() {
                    self.mutex.Lock()
                    defer self.mutex.Unlock()

                    packets, packetsErr = self.DataPackets(buffer, n, self.tcpBufferSettings.Mtu)
                    if packetsErr != nil {
                        return
                    }

                    if 1 < len(packets) {
                        fmt.Printf("SEGMENTED PACKETS (%d)\n", len(packets))
                    }
                    self.log("[f%d]receive(%d %d %d)", forwardIter, n, len(packets), self.receiveSeq)

                    // fmt.Printf("[multi] Receive window %d <> %d\n", uint32(self.receiveWindowSize), self.receiveSeq - self.receiveSeqAck + uint32(n))
                    for uint32(self.receiveWindowSize) < self.receiveSeq - self.receiveSeqAck + uint32(n) {
                        fmt.Printf("[multi] Receive window wait\n")
                        receiveAckCond.Wait()
                        select {
                        case <- self.ctx.Done():
                            return
                        default:
                        }
                    }

                    self.receiveSeq += uint32(n)
                }()

                select {
                case <- self.ctx.Done():
                    return
                default:
                }

                if packetsErr == nil {
                    for _, packet := range packets { 
                        receive(packet)
                    }
                }
                
                readTimeout = time.Now().Add(self.tcpBufferSettings.ReadTimeout)
            }
            
            if err != nil {
                if err == io.EOF {
                    // closed (FIN)
                    // propagate the FIN and close the sequence
                    self.log("[final]FIN")
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
                        self.log("[final]send FIN+ACK (%d)", self.sendSeq)
                        receive(packet)
                    }
                    return
                } else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                    if readTimeout.Before(time.Now()) {
                        fmt.Printf("[f%d]timeout\n", forwardIter)
                        return
                    }
                } else {
                    // some other error
                    return
                }
            }
        }
    }()

    for sendIter := 0; ; sendIter += 1 {
        checkpointId := self.idleCondition.Checkpoint()
        select {
        case <- self.ctx.Done():
            return
        case sendItem := <- self.sendItems:
            if "ACK" != tcpFlagsString(sendItem.tcp) {
                fmt.Printf("[r%d]receive(%d %s)\n", sendIter, len(sendItem.tcp.Payload), tcpFlagsString(sendItem.tcp))
            }

            drop := false
            seq := 0
            
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
                    self.receiveWindowSize = sendItem.tcp.Window
                    self.receiveSeqAck = sendItem.tcp.Ack
                    receiveAckCond.Broadcast()
                }
            }()

            if drop {
                continue
            }

            if sendItem.tcp.FIN {
                self.log("[r%d]FIN", sendIter)
                seq += 1
            }

            writeTimeout := time.Now().Add(self.tcpBufferSettings.WriteTimeout)

            payload := sendItem.tcp.Payload
            seq += len(payload)
            for i := 0; i < len(payload); {
                select {
                case <- self.ctx.Done():
                    return
                default:
                }

                socket.SetWriteDeadline(writeTimeout)
                n, err := socket.Write(payload[i:])

                if err == nil {
                    self.log("[r%d]forward(%d)", sendIter, n)
                } else {
                    fmt.Printf("[r%d]forward(%d err=%s)\n", sendIter, n, err)
                }

                if 0 < n {
                    j := i
                    i += n
                    fmt.Printf("[r%d]forward(%d/%d -> %d/%d +%d)\n", sendIter, j, len(payload), i, len(payload), n)
                }

                if err != nil {
                    if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                        if writeTimeout.Before(time.Now()) {
                            return
                        }
                    } else {
                        // some other error
                        return
                    }
                }
            }


            if 0 < seq {
                var packet []byte
                var err error
                func() {
                    self.mutex.Lock()
                    defer self.mutex.Unlock()

                    self.sendSeq += uint32(seq)
                    packet, err = self.PureAck()
                }()
                if err == nil {
                    receive(packet)
                }
            }

            


            if sendItem.tcp.FIN {
                // close the socket to propage the FIN and close the sequence
                socket.Close()
            }

            if sendItem.tcp.RST {
                // a RST typically appears for a bad TCP segment
                self.log("[r%d]RST", sendIter)
                return
            }

        case <- time.After(self.tcpBufferSettings.IdleTimeout):
            if self.idleCondition.Close(checkpointId) {
                // close the sequence
                self.log("[r%d]timeout", sendIter)
                return
            }
            // else there pending updates
        }
    }
}

type TcpSendItem struct {
    provideMode protocol.ProvideMode
    tcp *layers.TCP
}

type ConnectionState struct {
    source Path
    ipVersion int
    sourceIp net.IP
    sourcePort layers.TCPPort
    destinationIp net.IP
    destinationPort layers.TCPPort

    mutex sync.Mutex

    sendSeq uint32
    receiveSeq uint32
    receiveSeqAck uint32
    receiveWindowSize uint16
    windowSize uint16
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
            Version: 4,
            TTL: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            Protocol: layers.IPProtocolTCP,
        }
        headerSize += Ipv4HeaderSizeWithoutExtensions
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            NextHeader: layers.IPProtocolTCP,
        }
        headerSize += Ipv6HeaderSize
    }

    tcp := layers.TCP{
        SrcPort: self.destinationPort,
        DstPort: self.sourcePort,
        Seq: self.receiveSeq,
        Ack: self.sendSeq,
        ACK: true,
        SYN: true,
        Window: self.windowSize,
        // TODO window scale
        // https://datatracker.ietf.org/doc/html/rfc1323#page-8
    }
    tcp.SetNetworkLayerForChecksum(ip)
    headerSize += TcpHeaderSizeWithoutExtensions

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
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
            Version: 4,
            TTL: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            Protocol: layers.IPProtocolTCP,
        }
        headerSize += Ipv4HeaderSizeWithoutExtensions
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            NextHeader: layers.IPProtocolTCP,
        }
        headerSize += Ipv6HeaderSize
    }

    tcp := layers.TCP{
        SrcPort: self.destinationPort,
        DstPort: self.sourcePort,
        Seq: self.receiveSeq,
        Ack: self.sendSeq,
        ACK: true,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip)
    headerSize += TcpHeaderSizeWithoutExtensions

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
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
            Version: 4,
            TTL: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            Protocol: layers.IPProtocolTCP,
        }
        headerSize += Ipv4HeaderSizeWithoutExtensions
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            NextHeader: layers.IPProtocolTCP,
        }
        headerSize += Ipv6HeaderSize
    }

    tcp := layers.TCP{
        SrcPort: self.destinationPort,
        DstPort: self.sourcePort,
        Seq: self.receiveSeq,
        Ack: self.sendSeq,
        ACK: true,
        FIN: true,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip)
    headerSize += TcpHeaderSizeWithoutExtensions

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
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
            Version: 4,
            TTL: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            Protocol: layers.IPProtocolTCP,
        }
        headerSize += Ipv4HeaderSizeWithoutExtensions
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            NextHeader: layers.IPProtocolTCP,
        }
        headerSize += Ipv6HeaderSize
    }

    tcp := layers.TCP{
        SrcPort: self.destinationPort,
        DstPort: self.sourcePort,
        Seq: self.receiveSeq,
        Ack: self.sendSeq,
        ACK: true,
        RST: true,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip)
    headerSize += TcpHeaderSizeWithoutExtensions

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
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
            Version: 4,
            TTL: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            Protocol: layers.IPProtocolTCP,
        }
        headerSize += Ipv4HeaderSizeWithoutExtensions
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            NextHeader: layers.IPProtocolTCP,
        }
        headerSize += Ipv6HeaderSize
    }

    tcp := layers.TCP{
        SrcPort: self.destinationPort,
        DstPort: self.sourcePort,
        Seq: self.receiveSeq,
        Ack: self.sendSeq,
        ACK: true,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip)
    headerSize += TcpHeaderSizeWithoutExtensions

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
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

    if headerSize + n <= mtu {
        buffer := gopacket.NewSerializeBufferExpectedSize(headerSize + n, 0)
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
            j := min(i + packetSize, n)
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
    client *Client
    localUserNat *LocalUserNat
    securityPolicy *SecurityPolicy
    settings *RemoteUserNatProviderSettings
    localUserNatUnsub func()
    clientUnsub func()
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
        client: client,
        localUserNat: localUserNat,
        securityPolicy: DefaultSecurityPolicy(),
        settings: settings,
    }

    localUserNatUnsub := localUserNat.AddReceivePacketCallback(userNatProvider.Receive)
    userNatProvider.localUserNatUnsub = localUserNatUnsub
    clientUnsub := client.AddReceiveCallback(userNatProvider.ClientReceive)
    userNatProvider.clientUnsub = clientUnsub

    return userNatProvider
}

// `ReceivePacketFunction`
func (self *RemoteUserNatProvider) Receive(source Path, ipProtocol IpProtocol, packet []byte) {
    if self.client.ClientId() == source.ClientId {
        // locally generated traffic should use a separate local user nat
        fmt.Printf("drop remote user nat provider s packet ->%s\n", source.ClientId)
        return
    }

    // fmt.Printf("REMOTE USER NAT PROVIDER RETURN\n")

    ipPacketFromProvider := &protocol.IpPacketFromProvider{
        IpPacket: &protocol.IpPacket{
            PacketBytes: packet,
        },
    }
    frame, err := ToFrame(ipPacketFromProvider)
    if err != nil {
        fmt.Printf("drop remote user nat provider s packet ->%s = %s\n", source.ClientId, err)
        panic(err)
    }

    // fmt.Printf("remote user nat provider s packet ->%s\n", source.ClientId)

    // fmt.Printf("SEND PROTOCOL %s\n", ipProtocol)
    opts := []any{
        CompanionContract(),
    }
    switch ipProtocol {
    case IpProtocolUdp:
        opts = append(opts, NoAck())
    }
    TraceWithReturn(
        fmt.Sprintf("[unps] %s->%s", self.client.ClientTag(), source.ClientId.String()),
        func()(bool) {
            return self.client.SendWithTimeout(
                frame,
                source.ClientId,
                func(err error) {},
                self.settings.WriteTimeout,
                opts...,
            )
        },
    )
}

// `connect.ReceiveFunction`
func (self *RemoteUserNatProvider) ClientReceive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
    for _, frame := range frames {
        switch frame.MessageType {
        case protocol.MessageType_IpIpPacketToProvider:
            // fmt.Printf("REMOTE USER NAT PROVIDER SEND\n")

            ipPacketToProvider_, err := FromFrame(frame)
            if err != nil {
                panic(err)
            }
            ipPacketToProvider := ipPacketToProvider_.(*protocol.IpPacketToProvider)

            packet := ipPacketToProvider.IpPacket.PacketBytes
            _, r := self.securityPolicy.Inspect(provideMode, packet)
            ipLog("CLIENT RECEIVE %d -> %s", len(packet), r)
            switch r {
            case SecurityPolicyResultAllow:
                source := Path{ClientId: sourceId}
                TraceWithReturn(
                    fmt.Sprintf("[unpr] %s<-%s", self.client.ClientTag(), sourceId.String()),
                    func()(bool) {
                        return self.localUserNat.SendPacketWithTimeout(source, provideMode, packet, self.settings.WriteTimeout)
                    },
                )
            case SecurityPolicyResultIncident:
                self.client.ReportAbuse(sourceId)
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
    client *Client
    receivePacketCallback ReceivePacketFunction
    securityPolicy *SecurityPolicy
    pathTable *pathTable
    sourceFilter map[Path]bool
    // the provide mode of the source packets
    // for locally generated packets this is `ProvideMode_Network`
    provideMode protocol.ProvideMode
    clientUnsub func()
}

func NewRemoteUserNatClient(
    client *Client,
    receivePacketCallback ReceivePacketFunction,
    destinations []Path,
    provideMode protocol.ProvideMode,
) (*RemoteUserNatClient, error) {
    pathTable, err := newPathTable(destinations)
    if err != nil {
        return nil, err
    }

    sourceFilter := map[Path]bool{}
    for _, destination := range destinations {
        sourceFilter[destination] = true
    }

    userNatClient := &RemoteUserNatClient{
        client: client,
        receivePacketCallback: receivePacketCallback,
        securityPolicy: DefaultSecurityPolicy(),
        pathTable: pathTable,
        sourceFilter: sourceFilter,
        provideMode: provideMode,
    }

    clientUnsub := client.AddReceiveCallback(userNatClient.ClientReceive)
    userNatClient.clientUnsub = clientUnsub

    return userNatClient, nil
}

// `SendPacketFunction`
func (self *RemoteUserNatClient) SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
    // fmt.Printf("REMOTE USER NAT CLIENT SEND\n")

    minRelationship := max(provideMode, self.provideMode)

    ipPath, r := self.securityPolicy.Inspect(minRelationship, packet)
    switch r {
    case SecurityPolicyResultAllow:
        destination, err := self.pathTable.SelectDestination(packet)
        if err != nil {
            // drop
            // TODO log
            ipLog("NO DESTINATION (%s)", err)
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

        // fmt.Printf("remote user nat client s packet ->%s\n", destination.ClientId)

        // the sender will control transfer
        opts := []any{}
        switch ipPath.Protocol {
        case IpProtocolUdp:
            opts = append(opts, NoAck())
        }
        success := self.client.SendWithTimeout(frame, destination.ClientId, func(err error) {
            // TODO log if no ack
            ipLog("!! OUT PACKET (%s)", err)
        }, timeout, opts...)
        if !success {
            ipLog("!! OUT PACKET NOT SENT")
        }
        return success
    default:
        return false
    }
}

// `connect.ReceiveFunction`
func (self *RemoteUserNatClient) ClientReceive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
    source := Path{ClientId: sourceId}

    // only process frames from the destinations
    if allow := self.sourceFilter[source]; !allow {
        ipLog("PACKET FILTERED")
        return
    }

    for _, frame := range frames {
        switch frame.MessageType {
        case protocol.MessageType_IpIpPacketFromProvider:
            // fmt.Printf("REMOTE USER NAT CLIENT RETURN\n")
            
            ipPacketFromProvider_, err := FromFrame(frame)
            if err != nil {
                panic(err)
            }
            ipPacketFromProvider := ipPacketFromProvider_.(*protocol.IpPacketFromProvider)

            // fmt.Printf("remote user nat client r packet %s<-\n", source)

            HandleError(func() {
                self.receivePacketCallback(source, IpProtocolUnknown, ipPacketFromProvider.IpPacket.PacketBytes)
            })
        }
    }
}

func (self *RemoteUserNatClient) Close() {
    // self.client.RemoveReceiveCallback(self.clientCallbackId)
    self.clientUnsub()
}


type pathTable struct {
    destinations []Path

    // TODO clean up entries that haven't been used in some time
    paths4 map[Ip4Path]Path
    paths6 map[Ip6Path]Path
}

func newPathTable(destinations []Path) (*pathTable, error) {
    if len(destinations) == 0 {
        return nil, errors.New("No destinations.")
    }
    return &pathTable{
        destinations: destinations,
        paths4: map[Ip4Path]Path{},
        paths6: map[Ip6Path]Path{},
    }, nil
}

func (self *pathTable) SelectDestination(packet []byte) (Path, error) {
    if len(self.destinations) == 1 {
        return self.destinations[0], nil
    }

    ipPath, err := ParseIpPath(packet)
    if err != nil {
        return Path{}, err
    }
    switch ipPath.Version {
    case 4:
        ip4Path := ipPath.ToIp4Path()
        if path, ok := self.paths4[ip4Path]; ok {
            return path, nil
        }
        i := mathrand.Intn(len(self.destinations))
        path := self.destinations[i]
        self.paths4[ip4Path] = path
        return path, nil
    case 6:
        ip6Path := ipPath.ToIp6Path()
        if path, ok := self.paths6[ip6Path]; ok {
            return path, nil
        }
        i := mathrand.Intn(len(self.destinations))
        path := self.destinations[i]
        self.paths6[ip6Path] = path
        return path, nil
    default:
        // no support for this version
        return Path{}, fmt.Errorf("No support for ip version %d", ipPath.Version)
    }
}


type IpProtocol int
const (
    IpProtocolUnknown IpProtocol = 0
    IpProtocolTcp IpProtocol = 1
    IpProtocolUdp IpProtocol = 2
)


type IpPath struct {
    Version int
    Protocol IpProtocol
    SourceIp net.IP
    SourcePort int
    DestinationIp net.IP
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

            return &IpPath {
                Version: int(ipVersion),
                Protocol: IpProtocolUdp,
                SourceIp: ipv4.SrcIP,
                SourcePort: int(udp.SrcPort),
                DestinationIp: ipv4.DstIP,
                DestinationPort: int(udp.DstPort),
            }, nil
        case layers.IPProtocolTCP:
            tcp := layers.TCP{}
            tcp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

            return &IpPath {
                Version: int(ipVersion),
                Protocol: IpProtocolTcp,
                SourceIp: ipv4.SrcIP,
                SourcePort: int(tcp.SrcPort),
                DestinationIp: ipv4.DstIP,
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

            return &IpPath {
                Version: int(ipVersion),
                Protocol: IpProtocolUdp,
                SourceIp: ipv6.SrcIP,
                SourcePort: int(udp.SrcPort),
                DestinationIp: ipv6.DstIP,
                DestinationPort: int(udp.DstPort),
            }, nil
        case layers.IPProtocolTCP:
            tcp := layers.TCP{}
            tcp.DecodeFromBytes(ipv6.Payload, gopacket.NilDecodeFeedback)

            return &IpPath {
                Version: int(ipVersion),
                Protocol: IpProtocolTcp,
                SourceIp: ipv6.SrcIP,
                SourcePort: int(tcp.SrcPort),
                DestinationIp: ipv6.DstIP,
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
        Protocol: self.Protocol,
        SourceIp: sourceIp,
        SourcePort: self.SourcePort,
        DestinationIp: destinationIp,
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
        Protocol: self.Protocol,
        SourceIp: sourceIp,
        SourcePort: self.SourcePort,
        DestinationIp: destinationIp,
        DestinationPort: self.DestinationPort,
    }
}

func (self *IpPath) Source() *IpPath {
    return &IpPath{
        Protocol: self.Protocol,
        Version: self.Version,
        SourceIp: self.SourceIp,
        SourcePort: self.SourcePort,
    }
}

func (self *IpPath) Destination() *IpPath {
    return &IpPath{
        Protocol: self.Protocol,
        Version: self.Version,
        DestinationIp: self.DestinationIp,
        DestinationPort: self.DestinationPort,
    }
}


// comparable
type Ip4Path struct {
    Protocol IpProtocol
    SourceIp [4]byte
    SourcePort int
    DestinationIp [4]byte
    DestinationPort int
}

func (self *Ip4Path) Source() Ip4Path {
    return Ip4Path{
        Protocol: self.Protocol,
        SourceIp: self.SourceIp,
        SourcePort: self.SourcePort,
    }
}

func (self *Ip4Path) Destination() Ip4Path {
    return Ip4Path{
        Protocol: self.Protocol,
        DestinationIp: self.DestinationIp,
        DestinationPort: self.DestinationPort,
    }
}


// comparable
type Ip6Path struct {
    Protocol IpProtocol
    SourceIp [16]byte
    SourcePort int
    DestinationIp [16]byte
    DestinationPort int
}

func (self *Ip6Path) Source() Ip6Path {
    return Ip6Path{
        Protocol: self.Protocol,
        SourceIp: self.SourceIp,
        SourcePort: self.SourcePort,
    }
}

func (self *Ip6Path) Destination() Ip6Path {
    return Ip6Path{
        Protocol: self.Protocol,
        DestinationIp: self.DestinationIp,
        DestinationPort: self.DestinationPort,
    }
}


type SecurityPolicyResult int
const (
    SecurityPolicyResultDrop SecurityPolicyResult = 0
    SecurityPolicyResultAllow SecurityPolicyResult = 1
    SecurityPolicyResultIncident SecurityPolicyResult = 2
)


type SecurityPolicy struct {
}

func DefaultSecurityPolicy() *SecurityPolicy {
    return &SecurityPolicy{}
}

func (self *SecurityPolicy) Inspect(provideMode protocol.ProvideMode, packet []byte) (*IpPath, SecurityPolicyResult) {
    ipPath, err := ParseIpPath(packet)
    if err != nil {
        // back ip packet
        return ipPath, SecurityPolicyResultDrop
    }

    if protocol.ProvideMode_Public <= provideMode {
        // apply public rules:
        // - only public unicast network destinations
        // - block insecure or known unencrypted traffic

        if !isPublicUnicast(ipPath.DestinationIp) {
            return ipPath, SecurityPolicyResultIncident 
        }

        // block insecure or unencrypted traffic is implemented as a block list,
        // rather than an allow list.
        // Known insecure traffic and unencrypted is blocked.
        // This currently includes:
        // - port 80 (http)
        allow := func()(bool) {
            switch ipPath.DestinationPort {
            case 80:
                return false
            default:
                return true
            }
        }
        if !allow() {
            return ipPath, SecurityPolicyResultDrop
        }
    }

    return ipPath, SecurityPolicyResultAllow
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


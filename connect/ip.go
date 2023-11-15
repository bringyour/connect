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

    "github.com/google/gopacket"
    "github.com/google/gopacket/layers"

    // "google.golang.org/protobuf/proto"

    "bringyour.com/protocol"
)


/*
implements user-space NAT (UNAT) and packet inspection
The UNAT emulates a raw socket using user-space sockets.

*/


// FIXME tune this
const SendTimeout = 200 * time.Millisecond


var ipLog = LogFn(LogLevelDebug, "ip")

const DefaultMtu = 1440
const Ipv4HeaderSizeWithoutExtensions = 20
const Ipv6HeaderSize = 40
const UdpHeaderSize = 8
const TcpHeaderSizeWithoutExtensions = 20

const debugVerifyHeaders = false


func DefaultUdpBufferSettings() *UdpBufferSettings {
    return &UdpBufferSettings{
        ReadTimeout: 60 * time.Second,
        WriteTimeout: 60 * time.Second,
        ReadPollTimeout: 60 * time.Second,
        WritePollTimeout: 60 * time.Second,
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
        ReadTimeout: 60 * time.Second,
        WriteTimeout: 60 * time.Second,
        ReadPollTimeout: 60 * time.Second,
        WritePollTimeout: 60 * time.Second,
        IdleTimeout: 60 * time.Second,
        ChannelBufferSize: 32,
        Mtu: DefaultMtu,
        // avoid fragmentation
        ReadBufferSize: DefaultMtu - max(Ipv4HeaderSizeWithoutExtensions, Ipv6HeaderSize) - max(UdpHeaderSize, TcpHeaderSizeWithoutExtensions),
    }
    tcpBufferSettings.WindowSize = tcpBufferSettings.ChannelBufferSize * tcpBufferSettings.Mtu / 2
    return tcpBufferSettings
}



// send from a raw socket
type SendPacketFunction func(source Path, provideMode protocol.ProvideMode, packet []byte)


// receive into a raw socket
type ReceivePacketFunction func(source Path, ipProtocol IpProtocol, packet []byte)



// FIXME rename this to local user nat

// forwards packets using user space sockets
// this assumes transfer between the packet source and this is lossless and in order,
// so the protocol stack implementations do not implement any retransmit logic
type LocalUserNat struct {
    ctx context.Context
    cancel context.CancelFunc

    sendPackets chan *SendPacket

    udpBufferSettings *UdpBufferSettings
    tcpBufferSettings *TcpBufferSettings

    // receive callback
    receiveCallbacks *CallbackList[ReceivePacketFunction]
}

func NewLocalUserNatWithDefaults(ctx context.Context) *LocalUserNat {
    return NewLocalUserNat(ctx, 32)
}

func NewLocalUserNat(ctx context.Context, sendBufferSize int) *LocalUserNat {
    cancelCtx, cancel := context.WithCancel(ctx)

    localUserNat := &LocalUserNat{
        ctx: cancelCtx,
        cancel: cancel,
        sendPackets: make(chan *SendPacket, sendBufferSize),
        udpBufferSettings: DefaultUdpBufferSettings(),
        tcpBufferSettings: DefaultTcpBufferSettings(),
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
func (self *LocalUserNat) SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte) {
    self.SendPacketWithTimeout(source, provideMode, packet, -1)
}

// func (self *LocalUserNat) ReceiveN(source Path, provideMode protocol.ProvideMode, packet []byte, n int) {
//     self.Receive(source, provideMode, packet[0:n])
// }

func (self *LocalUserNat) AddReceivePacketCallback(receiveCallback ReceivePacketFunction) {
    self.receiveCallbacks.Add(receiveCallback)
}

func (self *LocalUserNat) RemoveReceivePacketCallback(receiveCallback ReceivePacketFunction) {
    self.receiveCallbacks.Remove(receiveCallback)
}

// `ReceivePacketFunction`
func (self *LocalUserNat) receive(source Path, ipProtocol IpProtocol, packet []byte) {
    for _, receiveCallback := range self.receiveCallbacks.Get() {
        receiveCallback(source, ipProtocol, packet)
    }
}

func (self *LocalUserNat) Run() {
    udp4Buffer := NewUdp4Buffer(self.ctx, self.receive, self.udpBufferSettings)
    udp6Buffer := NewUdp6Buffer(self.ctx, self.receive, self.udpBufferSettings)
    tcp4Buffer := NewTcp4Buffer(self.ctx, self.receive, self.tcpBufferSettings)
    tcp6Buffer := NewTcp6Buffer(self.ctx, self.receive, self.tcpBufferSettings)

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

                    udp4Buffer.send(
                        sendPacket.source,
                        sendPacket.provideMode,
                        &ipv4,
                        &udp,
                    )
                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

                    tcp4Buffer.send(
                        sendPacket.source,
                        sendPacket.provideMode,
                        &ipv4,
                        &tcp,
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

                    udp6Buffer.send(
                        sendPacket.source,
                        sendPacket.provideMode,
                        &ipv6,
                        &udp,
                    )
                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv6.Payload, gopacket.NilDecodeFeedback)

                    tcp6Buffer.send(
                        sendPacket.source,
                        sendPacket.provideMode,
                        &ipv6,
                        &tcp,
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
    ReadPollTimeout time.Duration
    WritePollTimeout time.Duration
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
        ipv4 *layers.IPv4, udp *layers.UDP) {
    bufferId := NewBufferId4(
        source,
        ipv4.SrcIP, int(udp.SrcPort),
        ipv4.DstIP, int(udp.DstPort),
    )

    self.udpSend(
        bufferId,
        ipv4.SrcIP,
        ipv4.DstIP,
        source,
        provideMode,
        4,
        udp,
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
        ipv6 *layers.IPv6, udp *layers.UDP) {
    bufferId := NewBufferId6(
        source,
        ipv6.SrcIP, int(udp.SrcPort),
        ipv6.DstIP, int(udp.DstPort),
    )

    self.udpSend(
        bufferId,
        ipv6.SrcIP,
        ipv6.DstIP,
        source,
        provideMode,
        6,
        udp,
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
) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    initSequence := func()(*UdpSequence) {
        sequence, ok := self.sequences[bufferId]
        if ok {
            return sequence
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

    if !initSequence().send(provideMode, udp) {
        // sequence closed
        delete(self.sequences, bufferId)
        initSequence().send(provideMode, udp)
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

func (self *UdpSequence) send(provideMode protocol.ProvideMode, udp *layers.UDP) (success bool) {
    if !self.idleCondition.UpdateOpen() {
        success = false
        return
    }
    defer self.idleCondition.UpdateClose()
    defer func() {
        // this means there was some error in sequence processing
        if err := recover(); err != nil {
            success = false
        }
    }()
    self.sendItems <- &UdpSendItem{
        provideMode: provideMode,
        udp: udp,
    }
    success = true
    return
}

func (self *UdpSequence) Run() {
    defer func() {
        self.Close()

        close(self.sendItems)
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
        defer self.Close()

        buffer := make([]byte, self.udpBufferSettings.ReadBufferSize)

        readTimeout := time.Now().Add(self.udpBufferSettings.ReadTimeout)
        for forwardIter := 0; ; forwardIter += 1 {
            select {
            case <- self.ctx.Done():
                return
            default:
            }

            deadline := MinTime(
                time.Now().Add(self.udpBufferSettings.ReadPollTimeout),
                readTimeout,
            )
            socket.SetReadDeadline(deadline)
            n, err := socket.Read(buffer)

            if err == nil {
                self.log("[f%d]send(%d)", forwardIter, n)
            } else {
                self.log("[f%d]send(%d err=%s)", forwardIter, n, err)
            }

            if 0 < n {
                packets, err := self.DataPackets(buffer, n, self.udpBufferSettings.Mtu)
                if err != nil {
                    return
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
                    continue
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

                deadline := MinTime(
                    time.Now().Add(self.udpBufferSettings.WritePollTimeout),
                    writeTimeout,
                )
                socket.SetWriteDeadline(deadline)
                n, err := socket.Write(payload)

                if err == nil {
                    self.log("[r%d]forward (%d)", sendIter, n)
                } else {
                    self.log("[r%d]forward (%d err=%s)", sendIter, n, err)
                }

                payload = payload[n:len(payload)]

                if err != nil {
                    if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                        if writeTimeout.Before(time.Now()) {
                            return
                        }
                        continue
                    } else {
                        // some other error
                        return
                    }
                }
                break
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
    ReadPollTimeout time.Duration
    WritePollTimeout time.Duration
    IdleTimeout time.Duration
    ReadBufferSize int
    ChannelBufferSize int
    Mtu int
    // the window size is the max amount of packet data in memory for each sequence
    // to avoid blocking, the window size should be smaller than the channel buffer byte size (channel buffer size * mean packet size)
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
        ipv4 *layers.IPv4, tcp *layers.TCP) {
    bufferId := NewBufferId4(
        source,
        ipv4.SrcIP, int(tcp.SrcPort),
        ipv4.DstIP, int(tcp.DstPort),
    )

    self.tcpSend(
        bufferId,
        ipv4.SrcIP,
        ipv4.DstIP,
        source,
        provideMode,
        4,
        tcp,
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
        ipv6 *layers.IPv6, tcp *layers.TCP) {
    bufferId := NewBufferId6(
        source,
        ipv6.SrcIP, int(tcp.SrcPort),
        ipv6.DstIP, int(tcp.DstPort),
    )

    self.tcpSend(
        bufferId,
        ipv6.SrcIP,
        ipv6.DstIP,
        source,
        provideMode,
        6,
        tcp,
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
) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    // new sequence
    if tcp.SYN {
        if sequence, ok := self.sequences[bufferId]; ok {
            sequence.Close()
            delete(self.sequences, bufferId)
        }
    }

    sequence, ok := self.sequences[bufferId]
    if !ok {
        if tcp.SYN {
            sequence = NewTcpSequence(
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
            return
        }
    }
    sequence.send(provideMode, tcp)
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
    connectionState := ConnectionState{
        source: source,
        ipVersion: ipVersion,
        sourceIp: sourceIp,
        sourcePort: sourcePort,
        destinationIp: destinationIp,
        destinationPort: destinationPort,
        // the window size starts at the fixed value
        windowSize: uint16(tcpBufferSettings.WindowSize),
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

func (self *TcpSequence) send(provideMode protocol.ProvideMode, tcp *layers.TCP) (success bool) {
    if !self.idleCondition.UpdateOpen() {
        success = false
        return
    }
    defer self.idleCondition.UpdateClose()
    defer func() {
        // this means there was some error in sequence processing
        if err := recover(); err != nil {
            success = false
        }
    }()
    self.sendItems <- &TcpSendItem{
        provideMode: provideMode,
        tcp: tcp,
    }
    success = true
    return
}

func (self *TcpSequence) Run() {
    defer func() {
        self.Close()

        close(self.sendItems)

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
    // send a final FIN+ACK
    defer func() {
        self.log("[final]FIN")
        self.mutex.Lock()
        packet, err := self.FinAck()
        if err != nil {
            return
        }
        self.log("[final]send FIN+ACK (%d)", self.sendSeq)
        receive(packet)
        self.mutex.Unlock()
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

                self.mutex.Lock()
                // sendSeq is the next expected sequence number
                // SYN and FIN consume one
                self.sendSeq = sendItem.tcp.Seq + 1
                // start the send seq at 0
                // this is arbitrary, and since there is no transport security risk back to sender is fine
                self.receiveSeq = 0
                packet, err := self.SynAck()
                if err != nil {
                    return
                }
                self.log("[init]receive SYN+ACK")
                receive(packet)
                self.receiveSeq += 1
                self.mutex.Unlock()
                
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
    // default is nodelay=true
    if err != nil {
        self.log("[init]connect error (%s)", err)
        return
    }
    defer socket.Close()

    self.log("[init]connect success")


    go func() {
        defer self.Close()

        buffer := make([]byte, self.tcpBufferSettings.ReadBufferSize)
        
        readTimeout := time.Now().Add(self.tcpBufferSettings.ReadTimeout)
        for forwardIter := 0; ; forwardIter += 1 {
            select {
            case <- self.ctx.Done():
                return
            default:
            }

            deadline := MinTime(
                time.Now().Add(self.tcpBufferSettings.ReadPollTimeout),
                readTimeout,
            )
            socket.SetReadDeadline(deadline)
            
            n, err := socket.Read(buffer)

            if err == nil {
                self.log("[f%d]send(%d)", forwardIter, n)
            } else {
                self.log("[f%d]send(%d) (err=%s)", forwardIter, n, err)
            }

            if 0 < n {
                // since the transfer from local to remove is lossless and preserves order,
                // do not worry about retransmits
                self.mutex.Lock()
                packets, err := self.DataPackets(buffer, n, self.tcpBufferSettings.Mtu)
                if err != nil {
                    return
                }
                self.log("[f%d]receive(%d %d %d)", forwardIter, n, len(packets), self.receiveSeq)
                self.receiveSeq += uint32(n)
                for _, packet := range packets {   
                    receive(packet)
                }
                self.mutex.Unlock()
                
                readTimeout = time.Now().Add(self.tcpBufferSettings.ReadTimeout)
            }
            
            if err != nil {
                if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                    if readTimeout.Before(time.Now()) {
                        self.log("[f%d]timeout", forwardIter)
                        return
                    }
                    continue
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
            self.log("[r%d]receive(%d %s)", sendIter, len(sendItem.tcp.Payload), tcpFlagsString(sendItem.tcp))

            if self.sendSeq != sendItem.tcp.Seq {
                // a retransmit
                // since the transfer from local to remote is lossless and preserves order,
                // the packet is already pending. Ignore.
                self.mutex.Lock()
                self.log("[r%d]retransmit (%d %d)", sendIter, self.sendSeq, sendItem.tcp.Seq)
                self.mutex.Unlock()
                continue
            }

            // ignore ACKs because we do not need to retransmit (see above)
            if sendItem.tcp.ACK {
                self.mutex.Lock()
                self.log("[r%d]ACK (%d %d)", sendIter, self.receiveSeq, sendItem.tcp.Ack)
                self.mutex.Unlock()
            }

            if sendItem.tcp.FIN {
                self.log("[r%d]FIN", sendIter)
                self.mutex.Lock()
                self.sendSeq += 1
                self.mutex.Unlock()
                return
            }

            if sendItem.tcp.RST {
                // a RST typically appears for a bad TCP segment
                self.log("[r%d]RST", sendIter)
                return
            }

            writeTimeout := time.Now().Add(self.tcpBufferSettings.WriteTimeout)

            payload := sendItem.tcp.Payload
            for 0 < len(payload) {
                select {
                case <- self.ctx.Done():
                    return
                default:
                }

                deadline := MinTime(
                    time.Now().Add(self.tcpBufferSettings.WritePollTimeout),
                    writeTimeout,
                )
                socket.SetWriteDeadline(deadline)
                n, err := socket.Write(payload)

                if err == nil {
                    self.log("[r%d]forward(%d)", sendIter, n)
                } else {
                    self.log("[r%d]forward(%d err=%s)", sendIter, n, err)
                }

                payload = payload[n:len(payload)]


                if 0 < n {
                    self.mutex.Lock()
                    self.sendSeq += uint32(n)
                    packet, err := self.PureAck()
                    if err != nil {
                        return
                    }
                    self.log("[r%d]receive ACK (%d)", sendIter, self.sendSeq)
                    receive(packet)
                    self.mutex.Unlock()
                }

                if err != nil {
                    if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                        if writeTimeout.Before(time.Now()) {
                            return
                        }
                        continue
                    } else {
                        // some other error
                        return
                    }
                }
                break
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


type RemoteUserNatProvider struct {
    client *Client
    localUserNat *LocalUserNat
    securityPolicy *SecurityPolicy
}

func NewRemoteUserNatProvider(client *Client, localUserNat *LocalUserNat) *RemoteUserNatProvider {
    userNatProvider := &RemoteUserNatProvider{
        client: client,
        localUserNat: localUserNat,
        securityPolicy: DefaultSecurityPolicy(),
    }

    localUserNat.AddReceivePacketCallback(userNatProvider.Receive)
    client.AddReceiveCallback(userNatProvider.ClientReceive)

    return userNatProvider
}

// `ReceivePacketFunction`
func (self *RemoteUserNatProvider) Receive(source Path, ipProtocol IpProtocol, packet []byte) {
    if self.client.ClientId() == source.ClientId {
        // locally generated traffic
        return
    }

    fmt.Printf("REMOTE USER NAT PROVIDER RETURN\n")

    ipPacketFromProvider := &protocol.IpPacketFromProvider{
        IpPacket: &protocol.IpPacket{
            PacketBytes: packet,
        },
    }
    frame, err := ToFrame(ipPacketFromProvider)
    if err != nil {
        panic(err)
    }

    // fmt.Printf("SEND PROTOCOL %s\n", ipProtocol)
    opts := []any{}
    switch ipProtocol {
    case IpProtocolUdp:
        opts = append(opts, NoAck())
    }
    success := self.client.SendWithTimeout(frame, source.ClientId, func(err error) {
        // TODO log
        ipLog("!! RETURN PACKET (%s)", err)
    }, SendTimeout, opts...)
    if !success {
        // TODO log
        ipLog("!! RETURN PACKET NOT SENT")
    }
}

// `connect.ReceiveFunction`
func (self *RemoteUserNatProvider) ClientReceive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
    for _, frame := range frames {
        switch frame.MessageType {
        case protocol.MessageType_IpIpPacketToProvider:
            fmt.Printf("REMOTE USER NAT PROVIDER SEND\n")

            ipPacketToProvider_, err := FromFrame(frame)
            if err != nil {
                panic(err)
            }
            ipPacketToProvider := ipPacketToProvider_.(*protocol.IpPacketToProvider)

            packet := ipPacketToProvider.IpPacket.PacketBytes
            r := self.securityPolicy.Inspect(provideMode, packet)
            ipLog("CLIENT RECEIVE %d -> %s", len(packet), r)
            switch r {
            case SecurityPolicyResultAllow:
                source := Path{ClientId: sourceId}
                success := self.localUserNat.SendPacketWithTimeout(source, provideMode, packet, SendTimeout)
                if !success {
                    // TODO log
                    ipLog("!! FORWARD PACKET NOT SENT")
                }
            case SecurityPolicyResultIncident:
                self.client.ReportAbuse(sourceId)
            }
        }
    }
}

func (self *RemoteUserNatProvider) Close() {
    self.client.RemoveReceiveCallback(self.ClientReceive)
    self.localUserNat.RemoveReceivePacketCallback(self.Receive)
}


type RemoteUserNatClient struct {
    client *Client
    receivePacketCallback ReceivePacketFunction
    securityPolicy *SecurityPolicy
    pathTable *PathTable
    sourceFilter map[Path]bool
    // the provide mode of the source packets
    // for locally generated packets this is `ProvideMode_Network`
    provideMode protocol.ProvideMode
}

func NewRemoteUserNatClient(
    client *Client,
    receivePacketCallback ReceivePacketFunction,
    destinations []Path,
    provideMode protocol.ProvideMode,
) (*RemoteUserNatClient, error) {
    pathTable, err := NewPathTable(destinations)
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

    client.AddReceiveCallback(userNatClient.ClientReceive)

    return userNatClient, nil
}

// `SendPacketFunction`
func (self *RemoteUserNatClient) SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte) {
    fmt.Printf("REMOTE USER NAT CLIENT SEND\n")

    minRelationship := max(provideMode, self.provideMode)

    r := self.securityPolicy.Inspect(minRelationship, packet)
    switch r {
    case SecurityPolicyResultAllow:
        destination, err := self.pathTable.SelectDestination(packet)
        if err != nil {
            // drop
            // TODO log
            ipLog("NO DESTINATION (%s)", err)
            return
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
        opts := []any{NoAck()}
        success := self.client.SendWithTimeout(frame, destination.ClientId, func(err error) {
            // TODO log if no ack
            ipLog("!! OUT PACKET (%s)", err)
        }, SendTimeout, opts...)
        if !success {
            ipLog("!! OUT PACKET NOT SENT")
        }
    }
}

// `connect.ReceiveFunction`
func (self *RemoteUserNatClient) ClientReceive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
    source := Path{ClientId: sourceId}

    // only process frames from the destinations
    if allow, ok := self.sourceFilter[source]; !ok || !allow {
        ipLog("PACKET FILTERED")
        return
    }

    for _, frame := range frames {
        switch frame.MessageType {
        case protocol.MessageType_IpIpPacketFromProvider:
            fmt.Printf("REMOTE USER NAT CLIENT RETURN\n")
            
            ipPacketFromProvider_, err := FromFrame(frame)
            if err != nil {
                panic(err)
            }
            ipPacketFromProvider := ipPacketFromProvider_.(*protocol.IpPacketFromProvider)

            self.receivePacketCallback(source, IpProtocolUnknown, ipPacketFromProvider.IpPacket.PacketBytes)
        }
    }
}

func (self *RemoteUserNatClient) Close() {
    self.client.RemoveReceiveCallback(self.ClientReceive)
}


type PathTable struct {
    destinations []Path

    // TODO clean up entries that haven't been used in some time
    paths4 map[ip4Path]Path
    paths6 map[ip6Path]Path
}

func NewPathTable(destinations []Path) (*PathTable, error) {
    if len(destinations) == 0 {
        return nil, errors.New("No destinations.")
    }
    return &PathTable{
        destinations: destinations,
        paths4: map[ip4Path]Path{},
        paths6: map[ip6Path]Path{},
    }, nil
}

func (self *PathTable) SelectDestination(packet []byte) (Path, error) {
    if len(self.destinations) == 1 {
        return self.destinations[0], nil
    }

    ipPath, err := parseIpPath(packet)
    if err != nil {
        return Path{}, err
    }
    switch ipPath.version {
    case 4:
        ip4Path := ipPath.toIp4Path()
        if path, ok := self.paths4[ip4Path]; ok {
            return path, nil
        }
        i := mathrand.Intn(len(self.destinations))
        path := self.destinations[i]
        self.paths4[ip4Path] = path
        return path, nil
    case 6:
        ip6Path := ipPath.toIp6Path()
        if path, ok := self.paths6[ip6Path]; ok {
            return path, nil
        }
        i := mathrand.Intn(len(self.destinations))
        path := self.destinations[i]
        self.paths6[ip6Path] = path
        return path, nil
    default:
        // no support for this version
        return Path{}, fmt.Errorf("No support for ip version %d", ipPath.version)
    }
}


type IpProtocol int
const (
    IpProtocolUnknown IpProtocol = 0
    IpProtocolTcp IpProtocol = 1
    IpProtocolUdp IpProtocol = 2
)


type ipPath struct {
    protocol IpProtocol
    version int
    sourceIp net.IP
    sourcePort int
    destinationIp net.IP
    destinationPort int
}

func parseIpPath(ipPacket []byte) (*ipPath, error) {
    ipVersion := uint8(ipPacket[0]) >> 4
    switch ipVersion {
    case 4:
        ipv4 := layers.IPv4{}
        ipv4.DecodeFromBytes(ipPacket, gopacket.NilDecodeFeedback)
        switch ipv4.Protocol {
        case layers.IPProtocolUDP:
            udp := layers.UDP{}
            udp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

            return &ipPath {
                protocol: IpProtocolUdp,
                version: int(ipVersion),
                sourceIp: ipv4.SrcIP,
                sourcePort: int(udp.SrcPort),
                destinationIp: ipv4.DstIP,
                destinationPort: int(udp.DstPort),
            }, nil
        case layers.IPProtocolTCP:
            tcp := layers.TCP{}
            tcp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

            return &ipPath {
                protocol: IpProtocolUdp,
                version: int(ipVersion),
                sourceIp: ipv4.SrcIP,
                sourcePort: int(tcp.SrcPort),
                destinationIp: ipv4.DstIP,
                destinationPort: int(tcp.DstPort),
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

            return &ipPath {
                protocol: IpProtocolUdp,
                version: int(ipVersion),
                sourceIp: ipv6.SrcIP,
                sourcePort: int(udp.SrcPort),
                destinationIp: ipv6.DstIP,
                destinationPort: int(udp.DstPort),
            }, nil
        case layers.IPProtocolTCP:
            tcp := layers.TCP{}
            tcp.DecodeFromBytes(ipv6.Payload, gopacket.NilDecodeFeedback)

            return &ipPath {
                protocol: IpProtocolUdp,
                version: int(ipVersion),
                sourceIp: ipv6.SrcIP,
                sourcePort: int(tcp.SrcPort),
                destinationIp: ipv6.DstIP,
                destinationPort: int(tcp.DstPort),
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

func (self *ipPath) toIp4Path() ip4Path {
    return ip4Path{
        protocol: self.protocol,
        sourceIp: [4]byte(self.sourceIp),
        sourcePort: self.sourcePort,
        destinationIp: [4]byte(self.destinationIp),
        destinationPort: self.destinationPort,
    }
}

func (self *ipPath) toIp6Path() ip6Path {
    return ip6Path{
        protocol: self.protocol,
        sourceIp: [16]byte(self.sourceIp),
        sourcePort: self.sourcePort,
        destinationIp: [16]byte(self.destinationIp),
        destinationPort: self.destinationPort,
    }
}


// comparable
type ip4Path struct {
    protocol IpProtocol
    sourceIp [4]byte
    sourcePort int
    destinationIp [4]byte
    destinationPort int
}


// comparable
type ip6Path struct {
    protocol IpProtocol
    sourceIp [16]byte
    sourcePort int
    destinationIp [16]byte
    destinationPort int
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

func (self *SecurityPolicy) Inspect(provideMode protocol.ProvideMode, packet []byte) SecurityPolicyResult {
    ipPath, err := parseIpPath(packet)
    if err != nil {
        // back ip packet
        return SecurityPolicyResultDrop
    }

    if protocol.ProvideMode_Public <= provideMode {
        // apply public rules:
        // - only public unicast network destinations
        // - block insecure or known unencrypted traffic

        if !isPublicUnicast(ipPath.destinationIp) {
            return SecurityPolicyResultIncident 
        }

        // block insecure or unencrypted traffic is implemented as a block list,
        // rather than an allow list.
        // Known insecure traffic and unencrypted is blocked.
        // This currently includes:
        // - port 80 (http)
        allow := func()(bool) {
            switch ipPath.destinationPort {
            case 80:
                return false
            default:
                return true
            }
        }
        if !allow() {
            return SecurityPolicyResultDrop
        }
    }

    return SecurityPolicyResultAllow
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


package connect

import (
    "net"
    "context"
    "time"
    "sync"
    "strconv"
    "fmt"
    "strings"

    "github.com/google/gopacket"
    "github.com/google/gopacket/layers"

    "bringyour.com/protocol"
)


var ipLog = LogFn("ip")


// TODO as a test call RemoteUserNat directly from an Android VPN
// TODO make sure to exclude the app from the vpn by setting the network of the app to null

// TODO LocalUserNat received packet, runs security checks, and then calls callback to pass on the packet


// apply userspacenat

// buffer (processes raw packets in flight):
// 1. require tls
// 2. inspect sni. reject self signed certs. extract stream information and expose it
// 3. 

// type AnalysisEvent 
// source Path
// - errors, transfer stats, etc



// TODO
// - tcp timeout
// - mtu calculation
// - udp logging
// - (DONE; this is all logging overhead) udp performance



func DefaultUdpBufferSettings() *UdpBufferSettings {
    return &UdpBufferSettings{
        ReadTimeout: 60 * time.Second,
        WriteTimeout: 60 * time.Second,
        ReadPollTimeout: 60 * time.Second,
        WritePollTimeout: 60 * time.Second,
        IdleTimeout: 60 * time.Second,
        // FIXME fix packet fragmentation to consider the MTU with headers
        Mtu: 1400,
        ReadBufferSize: 4096,
        ChannelBufferSize: 32,
        // FIXME write buffer, read buffer
    }
}

func DefaultTcpBufferSettings() *TcpBufferSettings {
    tcpBufferSettings := &TcpBufferSettings{
        ConnectTimeout: 60 * time.Second,
        ReadTimeout: 60 * time.Second,
        WriteTimeout: 60 * time.Second,
        ReadPollTimeout: 60 * time.Second,
        WritePollTimeout: 60 * time.Second,
        ReadBufferSize: 4096,
        ChannelBufferSize: 32,
        // FIXME fix packet fragmentation to consider the MTU with headers
        Mtu: 1400,
    }
    tcpBufferSettings.WindowSize = tcpBufferSettings.ChannelBufferSize * tcpBufferSettings.Mtu / 2
    return tcpBufferSettings
}



// receive from a raw socket
type ReceivePacketFunction func(source Path, provideMode protocol.ProvideMode, packet []byte)


// send to a raw socket
type SendPacketFunction func(destination Path, packet []byte)



// forwards packets using user space sockets
// this assumes transfer between the packet source and this is lossless and in order,
// so the protocol stack implementations do not implement any retransmit logic
type RemoteUserNat struct {
    ctx context.Context
    cancel context.CancelFunc

    receivePackets chan *ReceivePacket

    udpBufferSettings *UdpBufferSettings
    tcpBufferSettings *TcpBufferSettings

    // send callback
    sendCallbacks *CallbackList[SendPacketFunction]
}

func NewRemoteUserNatWithDefaults(ctx context.Context) *RemoteUserNat {
    return NewRemoteUserNat(ctx, 32)
}

func NewRemoteUserNat(ctx context.Context, receiveBufferSize int) *RemoteUserNat {
    cancelCtx, cancel := context.WithCancel(ctx)

    return &RemoteUserNat{
        ctx: cancelCtx,
        cancel: cancel,
        receivePackets: make(chan *ReceivePacket, receiveBufferSize),
        udpBufferSettings: DefaultUdpBufferSettings(),
        tcpBufferSettings: DefaultTcpBufferSettings(),
        sendCallbacks: NewCallbackList[SendPacketFunction](),
    }
}

// TODO provide mode of the destination determines filtering rules - e.g. local networks
// TODO currently filter all local networks and non-encrypted traffic
func (self *RemoteUserNat) ReceiveWithTimeout(source Path, provideMode protocol.ProvideMode,
        packet []byte, timeout time.Duration) bool {
    ipLog("ReceiveWithTimeout(%s, %s, %d, %s)", source, provideMode, len(packet), timeout)
    receivePacket := &ReceivePacket{
        source: source,
        provideMode: provideMode,
        packet: packet,
    }
    if timeout < 0 {
        self.receivePackets <- receivePacket
        return true
    } else if 0 == timeout {
        select {
        case self.receivePackets <- receivePacket:
            return true
        default:
            // full
            return false
        }
    } else {
        select {
        case <- self.ctx.Done():
            return false
        case self.receivePackets <- receivePacket:
            return true
        case <- time.After(timeout):
            // full
            return false
        }
    }
}

func (self *RemoteUserNat) Receive(source Path, provideMode protocol.ProvideMode, packet []byte) {
    self.ReceiveWithTimeout(source, provideMode, packet, -1)
}

func (self *RemoteUserNat) ReceiveN(source Path, provideMode protocol.ProvideMode, packet []byte, n int) {
    self.Receive(source, provideMode, packet[0:n])
}

func (self *RemoteUserNat) AddSendPacketCallback(sendCallback SendPacketFunction) {
    self.sendCallbacks.Add(sendCallback)
}

func (self *RemoteUserNat) RemoveSendPacketCallback(sendCallback SendPacketFunction) {
    self.sendCallbacks.Remove(sendCallback)
}

// SendPacketFunction
func (self *RemoteUserNat) send(destination Path, packet []byte) {
    for _, sendCallback := range self.sendCallbacks.Get() {
        sendCallback(destination, packet)
    }
}

func (self *RemoteUserNat) Run() {
    udp4Buffer := NewUdp4Buffer(self.ctx, self.send, self.udpBufferSettings)
    udp6Buffer := NewUdp6Buffer(self.ctx, self.send, self.udpBufferSettings)
    tcp4Buffer := NewTcp4Buffer(self.ctx, self.send, self.tcpBufferSettings)
    tcp6Buffer := NewTcp6Buffer(self.ctx, self.send, self.tcpBufferSettings)

    for {
        select {
        case <- self.ctx.Done():
            return
        case receivePacket := <- self.receivePackets:
            ipPacket := receivePacket.packet
            ipVersion := uint8(ipPacket[0]) >> 4
            switch ipVersion {
            case 4:
                ipv4 := layers.IPv4{}
                ipv4.DecodeFromBytes(ipPacket, gopacket.NilDecodeFeedback)
                switch ipv4.Protocol {
                case layers.IPProtocolUDP:
                    udp := layers.UDP{}
                    udp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

                    udp4Buffer.receive(
                        receivePacket.source,
                        receivePacket.provideMode,
                        &ipv4,
                        &udp,
                    )
                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv4.Payload, gopacket.NilDecodeFeedback)

                    tcp4Buffer.receive(
                        receivePacket.source,
                        receivePacket.provideMode,
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

                    udp6Buffer.receive(
                        receivePacket.source,
                        receivePacket.provideMode,
                        &ipv6,
                        &udp,
                    )
                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv6.Payload, gopacket.NilDecodeFeedback)

                    tcp6Buffer.receive(
                        receivePacket.source,
                        receivePacket.provideMode,
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

func (self *RemoteUserNat) Close() {
    self.cancel()
}

type ReceivePacket struct {
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

func NewUdp4Buffer(ctx context.Context, sendCallback SendPacketFunction,
        udpBufferSettings *UdpBufferSettings) *Udp4Buffer {
    return &Udp4Buffer{
        UdpBuffer: UdpBuffer[BufferId4]{
            ctx: ctx,
            sendCallback: sendCallback,
            udpBufferSettings: udpBufferSettings,
            sequences: map[BufferId4]*UdpSequence{},
        },
    }
}

func (self *Udp4Buffer) receive(source Path, provideMode protocol.ProvideMode,
        ipv4 *layers.IPv4, udp *layers.UDP) {
    bufferId := NewBufferId4(
        source,
        ipv4.SrcIP, int(udp.SrcPort),
        ipv4.DstIP, int(udp.DstPort),
    )

    self.udpReceive(
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

func NewUdp6Buffer(ctx context.Context, sendCallback SendPacketFunction,
        udpBufferSettings *UdpBufferSettings) *Udp6Buffer {
    return &Udp6Buffer{
        UdpBuffer: UdpBuffer[BufferId6]{
            ctx: ctx,
            sendCallback: sendCallback,
            udpBufferSettings: udpBufferSettings,
            sequences: map[BufferId6]*UdpSequence{},
        },
    }
}

func (self *Udp6Buffer) receive(source Path, provideMode protocol.ProvideMode,
        ipv6 *layers.IPv6, udp *layers.UDP) {
    bufferId := NewBufferId6(
        source,
        ipv6.SrcIP, int(udp.SrcPort),
        ipv6.DstIP, int(udp.DstPort),
    )

    self.udpReceive(
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
    sendCallback SendPacketFunction
    udpBufferSettings *UdpBufferSettings

    mutex sync.Mutex

    sequences map[BufferId]*UdpSequence
}


func (self *UdpBuffer[BufferId]) udpReceive(
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
            self.sendCallback,
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

    if !initSequence().receive(provideMode, udp) {
        // sequence closed
        delete(self.sequences, bufferId)
        initSequence().receive(provideMode, udp)
    }
}



type UdpSequence struct {
    ctx context.Context
    cancel context.CancelFunc
    sendCallback SendPacketFunction
    udpBufferSettings *UdpBufferSettings

    receiveItems chan *UpdReceiveItem

    idleCondition *IdleCondition

    StreamState
}

func NewUdpSequence(ctx context.Context, sendCallback SendPacketFunction,
        source Path, 
        ipVersion int,
        sourceIp net.IP, sourcePort layers.UDPPort,
        destinationIp net.IP, destinationPort layers.UDPPort,
        udpBufferSettings *UdpBufferSettings) *UdpSequence {
    cancelCtx, cancel := context.WithCancel(ctx)
    return &UdpSequence{
    //     log: SubLogFn(ipLog, fmt.Sprintf(
    //         "UdpSequence(%s %s)",
    //         streamState.SourceAuthority(),
    //         streamState.DestinationAuthority(),
    //     )),
        ctx: cancelCtx,
        cancel: cancel,
        sendCallback: sendCallback,
        receiveItems: make(chan *UpdReceiveItem, udpBufferSettings.ChannelBufferSize),
        udpBufferSettings: udpBufferSettings,
        idleCondition: NewIdleCondition(),
        StreamState: StreamState{
            source: source,
            ipVersion: ipVersion,
            sourceIp: sourceIp,
            sourcePort: sourcePort,
            destinationIp: destinationIp,
            destinationPort: destinationPort,
        },
    }
}

func (self *UdpSequence) receive(provideMode protocol.ProvideMode, udp *layers.UDP) (success bool) {
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
    self.receiveItems <- &UpdReceiveItem{
        provideMode: provideMode,
        udp: udp,
    }
    success = true
    return
}

func (self *UdpSequence) Run() {
    defer func() {
        self.Close()

        close(self.receiveItems)
        // drain and drop
        func() {
            for {
                select {
                case _, ok := <- self.receiveItems:
                    if !ok {
                        return
                    }
                }
            }
        }()
    }()

    socket, err := net.Dial(
        "udp",
        net.JoinHostPort(self.destinationIp.String(), strconv.Itoa(int(self.destinationPort))),
    )
    if err != nil {
        return
    }
    defer socket.Close()

    // socket.(*net.UDPConn).SetWriteBuffer(1024 * 16)
    // socket.(*net.UDPConn).SetReadBuffer(1024 * 16)
    

    go func() {
        defer self.Close()

        buffer := make([]byte, self.udpBufferSettings.ReadBufferSize)

        readTimeout := time.Now().Add(self.udpBufferSettings.ReadTimeout)
        for {
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

            if 0 < n {
                // FIXME forming the packets should take the mtu, since the mtu calculation includes the header size
                for i := 0; i < n; i += self.udpBufferSettings.Mtu {
                    j := min(i + self.udpBufferSettings.Mtu, n)
                    packet, err := self.DataPacket(buffer[i:j])
                    if err != nil {
                        return
                    }
                    self.sendCallback(self.source, packet)
                }
                readTimeout = time.Now().Add(self.udpBufferSettings.ReadTimeout)
            }

            if err != nil {
                if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                    if readTimeout.Before(time.Now()) {
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

    for {
        checkpointId := self.idleCondition.Checkpoint()
        select {
        case <- self.ctx.Done():
            return
        case receiveItem := <- self.receiveItems:
            writeTimeout := time.Now().Add(self.udpBufferSettings.WriteTimeout)

            payload := receiveItem.udp.Payload
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
                return
            }
            // else there pending updates
        }
    }
}

func (self *UdpSequence) Close() {
    self.cancel()
}

type UpdReceiveItem struct {
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
func (self *StreamState) DataPacket(payload []byte) ([]byte, error) {
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
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            NextHeader: layers.IPProtocolUDP,
        }
    }

    udp := layers.UDP{
        SrcPort: self.destinationPort,
        DstPort: self.sourcePort,
    }
    udp.SetNetworkLayerForChecksum(ip)

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        // FixLengths: true,
    }

    buffer := gopacket.NewSerializeBuffer()

    err := gopacket.SerializeLayers(buffer, options,
        ip.(gopacket.SerializableLayer),
        &udp,
        gopacket.Payload(payload),
    )

    if err != nil {
        return nil, err
    }
    packet := buffer.Bytes()
    return packet, nil
}


type TcpBufferSettings struct {
    ConnectTimeout time.Duration
    ReadTimeout time.Duration
    WriteTimeout time.Duration
    ReadPollTimeout time.Duration
    WritePollTimeout time.Duration
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

func NewTcp4Buffer(ctx context.Context, sendCallback SendPacketFunction,
        tcpBufferSettings *TcpBufferSettings) *Tcp4Buffer {
    return &Tcp4Buffer{
        TcpBuffer: TcpBuffer[BufferId4]{
            ctx: ctx,
            sendCallback: sendCallback,
            tcpBufferSettings: tcpBufferSettings,
            sequences: map[BufferId4]*TcpSequence{},
        },
    }
}

func (self *Tcp4Buffer) receive(source Path, provideMode protocol.ProvideMode, 
        ipv4 *layers.IPv4, tcp *layers.TCP) {
    bufferId := NewBufferId4(
        source,
        ipv4.SrcIP, int(tcp.SrcPort),
        ipv4.DstIP, int(tcp.DstPort),
    )

    self.tcpReceive(
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

func NewTcp6Buffer(ctx context.Context, sendCallback SendPacketFunction,
        tcpBufferSettings *TcpBufferSettings) *Tcp6Buffer {
    return &Tcp6Buffer{
        TcpBuffer: TcpBuffer[BufferId6]{
            ctx: ctx,
            sendCallback: sendCallback,
            tcpBufferSettings: tcpBufferSettings,
            sequences: map[BufferId6]*TcpSequence{},
        },
    }
}

func (self *Tcp6Buffer) receive(source Path, provideMode protocol.ProvideMode, 
        ipv6 *layers.IPv6, tcp *layers.TCP) {
    bufferId := NewBufferId6(
        source,
        ipv6.SrcIP, int(tcp.SrcPort),
        ipv6.DstIP, int(tcp.DstPort),
    )

    self.tcpReceive(
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
    sendCallback SendPacketFunction
    tcpBufferSettings *TcpBufferSettings

    mutex sync.Mutex

    sequences map[BufferId]*TcpSequence
}


func (self *TcpBuffer[BufferId]) tcpReceive(
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
                self.sendCallback,
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
    sequence.receive(provideMode, tcp)
}



type TcpSequence struct {
    log LogFunction
    ctx context.Context
    cancel context.CancelFunc
    
    sendCallback SendPacketFunction

    tcpBufferSettings *TcpBufferSettings

    receiveItems chan *TcpReceiveItem

    idleCondition *IdleCondition

    ConnectionState
}

func NewTcpSequence(ctx context.Context, sendCallback SendPacketFunction,
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
        log: SubLogFn(ipLog, fmt.Sprintf(
            "TcpSequence(%s %s)",
            connectionState.SourceAuthority(),
            connectionState.DestinationAuthority(),
        )),
        ctx: cancelCtx,
        cancel: cancel,
        sendCallback: sendCallback,
        tcpBufferSettings: tcpBufferSettings,
        receiveItems: make(chan *TcpReceiveItem, tcpBufferSettings.ChannelBufferSize),
        idleCondition: NewIdleCondition(),
        ConnectionState: connectionState,
    }
}

func (self *TcpSequence) receive(provideMode protocol.ProvideMode, tcp *layers.TCP) (success bool) {
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
    self.receiveItems <- &TcpReceiveItem{
        provideMode: provideMode,
        tcp: tcp,
    }
    success = true
    return
}

func (self *TcpSequence) Run() {
    defer func() {
        self.Close()

        close(self.receiveItems)

        // drain and drop
        func() {
            for {
                select {
                case _, ok := <- self.receiveItems:
                    if !ok {
                        return
                    }
                }
            }
        }()
    }()


    send := func(packet []byte) {
        self.sendCallback(self.source, packet)
    }
    // send a final FIN+ACK
    defer func() {
        self.log("[final]FIN")
        self.mutex.Lock()
        packet, err := self.FinAck()
        if err != nil {
            return
        }
        self.log("[final]send FIN+ACK (%d)", self.receiveSeq)
        send(packet)
        self.mutex.Unlock()
    }()


    for syn := false; !syn; {
        select {
        case <- self.ctx.Done():
            return
        case receiveItem := <- self.receiveItems:
            self.log("[init]receive(%d)", len(receiveItem.tcp.BaseLayer.Payload))
            // the first packet must be a syn
            if receiveItem.tcp.SYN {
                self.log("[init]SYN")

                self.mutex.Lock()
                // receiveSeq is the next expected sequence number
                // SYN and FIN consume one
                self.receiveSeq = receiveItem.tcp.Seq + 1
                // start the send seq at 0
                // this is arbitrary, and since there is no transport security risk back to sender is fine
                self.sendSeq = 0
                packet, err := self.SynAck()
                if err != nil {
                    return
                }
                self.log("[init]send SYN+ACK")
                send(packet)
                self.sendSeq += 1
                self.mutex.Unlock()
                
                syn = true
            } else {
                // an ACK here could be for a previous FIN
                self.log("[init]waiting for SYN (%s)", tcpFlagsString(receiveItem.tcp))
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
                self.log("[f%d]receive(%d)", forwardIter, n)
            } else {
                self.log("[f%d]receive(%d) (err=%s)", forwardIter, n, err)
            }

            if 0 < n {

                for i := 0; i < n; {
                    // FIXME we are not receiving acks
                    self.mutex.Lock()
                    // since the transfer from local to remove is lossless and preserves order,
                    // do not worry about retransmits
                    j := min(i + self.tcpBufferSettings.Mtu, n)
                    payload := buffer[i:j]
                    packet, err := self.DataPacket(payload)
                    if err != nil {
                        return
                    }
                    self.sendSeq += uint32(len(payload))
                    self.log("[f%d]send(%d) (%d)", forwardIter, len(payload), self.sendSeq)
                    send(packet)
                    self.mutex.Unlock()
                    i = j
                }

                readTimeout = time.Now().Add(self.tcpBufferSettings.ReadTimeout)
            }
            
            if err != nil {
                if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                    if readTimeout.Before(time.Now()) {
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

    // FIXME idle condition
    for receiveIter := 0; ; receiveIter += 1 {
        select {
        case <- self.ctx.Done():
            return
        case receiveItem := <- self.receiveItems:
            self.log("[r%d]receive(%d %s)", receiveIter, len(receiveItem.tcp.Payload), tcpFlagsString(receiveItem.tcp))

            

            if self.receiveSeq != receiveItem.tcp.Seq {
                // a retransmit
                // since the transfer from local to remote is lossless and preserves order,
                // the packet is already pending. Ignore.
                self.mutex.Lock()
                self.log("[r%d]retransmit (%d %d)", receiveIter, self.receiveSeq, receiveItem.tcp.Seq)
                self.mutex.Unlock()
                continue
            }

            // ignore ACKs because we do not need to retransmit (see above)
            if receiveItem.tcp.ACK {
                self.mutex.Lock()
                self.log("[r%d]ACK (%d %d)", receiveIter, self.sendSeq, receiveItem.tcp.Ack)
                self.mutex.Unlock()
            }

            if receiveItem.tcp.FIN {
                self.log("[r%d]FIN", receiveIter)
                self.mutex.Lock()
                self.receiveSeq += 1
                self.mutex.Unlock()
                return
            }

            if receiveItem.tcp.RST {
                // a RST typically appears for a bad TCP segment
                self.log("[r%d]RST", receiveIter)
                return
            }

            writeTimeout := time.Now().Add(self.tcpBufferSettings.WriteTimeout)

            payload := receiveItem.tcp.Payload
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
                self.log("[r%d]forward(%d)", receiveIter, n)
                payload = payload[n:len(payload)]


                if 0 < n {
                    self.mutex.Lock()
                    self.receiveSeq += uint32(n)
                    packet, err := self.PureAck()
                    if err != nil {
                        return
                    }
                    self.log("[r%d]send ACK (%d)", receiveIter, self.receiveSeq)
                    send(packet)
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
        }
    }
}

type TcpReceiveItem struct {
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

    receiveSeq uint32
    sendSeq uint32
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
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            NextHeader: layers.IPProtocolTCP,
        }
    }

    tcp := layers.TCP{
        SrcPort: self.destinationPort,
        DstPort: self.sourcePort,
        Seq: self.sendSeq,
        Ack: self.receiveSeq,
        ACK: true,
        SYN: true,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip)

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
    }

    buffer := gopacket.NewSerializeBuffer()

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
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            NextHeader: layers.IPProtocolTCP,
        }
    }

    tcp := layers.TCP{
        SrcPort: self.destinationPort,
        DstPort: self.sourcePort,
        Seq: self.sendSeq,
        Ack: self.receiveSeq,
        ACK: true,
        FIN: true,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip)

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
    }

    buffer := gopacket.NewSerializeBuffer()

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
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            NextHeader: layers.IPProtocolTCP,
        }
    }

    tcp := layers.TCP{
        SrcPort: self.destinationPort,
        DstPort: self.sourcePort,
        Seq: self.sendSeq,
        Ack: self.receiveSeq,
        ACK: true,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip)

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
    }

    buffer := gopacket.NewSerializeBuffer()

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

func (self *ConnectionState) DataPacket(payload []byte) ([]byte, error) {
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
    case 6:
        ip = &layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.destinationIp,
            DstIP: self.sourceIp,
            NextHeader: layers.IPProtocolTCP,
        }
    }

    tcp := layers.TCP{
        SrcPort: self.destinationPort,
        DstPort: self.sourcePort,
        Seq: self.sendSeq,
        Ack: self.receiveSeq,
        ACK: true,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip)

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
    }

    buffer := gopacket.NewSerializeBuffer()

    err := gopacket.SerializeLayers(buffer, options,
        ip.(gopacket.SerializableLayer),
        &tcp,
        gopacket.Payload(payload),
    )

    if err != nil {
        return nil, err
    }
    packet := buffer.Bytes()
    return packet, nil
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
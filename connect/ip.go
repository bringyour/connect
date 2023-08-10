package connect

import (
    "net"
    "context"
    "time"
    "sync"
    "strconv"

    "github.com/google/gopacket"
    "github.com/google/gopacket/layers"

    "bringyour.com/protocol"
)



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



func DefaultUdpBufferSettings() *UdpBufferSettings {
    return nil
}

func DefaultTcpBufferSettings() *TcpBufferSettings {
    return nil
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
                    udp.DecodeFromBytes(ipv4.BaseLayer.Payload, gopacket.NilDecodeFeedback)

                    udp4Buffer.receive(
                        receivePacket.source,
                        receivePacket.provideMode,
                        &ipv4,
                        &udp,
                    )
                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv4.BaseLayer.Payload, gopacket.NilDecodeFeedback)

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
                    udp.DecodeFromBytes(ipv6.BaseLayer.Payload, gopacket.NilDecodeFeedback)

                    udp6Buffer.receive(
                        receivePacket.source,
                        receivePacket.provideMode,
                        &ipv6,
                        &udp,
                    )
                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv6.BaseLayer.Payload, gopacket.NilDecodeFeedback)

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
    SendMtu int
    ReadBufferSize int
    ChannelBufferSize int
}


type Udp4Buffer struct {
    ctx context.Context
    sendCallback SendPacketFunction
    udpBufferSettings *UdpBufferSettings

    mutex sync.Mutex

    sequences map[BufferId4]*UdpSequence
}

func NewUdp4Buffer(ctx context.Context, sendCallback SendPacketFunction,
        udpBufferSettings *UdpBufferSettings) *Udp4Buffer {
    return &Udp4Buffer{
        ctx: ctx,
        sendCallback: sendCallback,
        udpBufferSettings: udpBufferSettings,
        sequences: map[BufferId4]*UdpSequence{},
    }
}

func (self *Udp4Buffer) receive(source Path, provideMode protocol.ProvideMode,
        ipv4 *layers.IPv4, udp *layers.UDP) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    bufferId := NewBufferId4(source, ipv4.SrcIP, int(udp.SrcPort), ipv4.DstIP, int(udp.DstPort))

    initSequence := func()(*UdpSequence) {
        sequence, ok := self.sequences[bufferId]
        if ok {
            return sequence
        }
        sequence = NewUdpSequence(
            self.ctx,
            self.sendCallback,
            source,
            4,
            ipv4.SrcIP,
            udp.SrcPort,
            ipv4.DstIP,
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


type Udp6Buffer struct {
    ctx context.Context
    sendCallback SendPacketFunction
    udpBufferSettings *UdpBufferSettings

    mutex sync.Mutex

    sequences map[BufferId6]*UdpSequence
}

func NewUdp6Buffer(ctx context.Context, sendCallback SendPacketFunction,
        udpBufferSettings *UdpBufferSettings) *Udp6Buffer {
    return &Udp6Buffer{
        ctx: ctx,
        sendCallback: sendCallback,
        udpBufferSettings: udpBufferSettings,
        sequences: map[BufferId6]*UdpSequence{},
    }
}

func (self *Udp6Buffer) receive(source Path, provideMode protocol.ProvideMode,
        ipv6 *layers.IPv6, udp *layers.UDP) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    bufferId := NewBufferId6(source, ipv6.SrcIP, int(udp.SrcPort), ipv6.DstIP, int(udp.DstPort))

    initSequence := func()(*UdpSequence) {
        sequence, ok := self.sequences[bufferId]
        if ok {
            return sequence
        }
        sequence = NewUdpSequence(
            self.ctx,
            self.sendCallback,
            source,
            4,
            ipv6.SrcIP,
            udp.SrcPort,
            ipv6.DstIP,
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
                for i := 0; i < n; i += self.udpBufferSettings.SendMtu {
                    j := min(i + self.udpBufferSettings.SendMtu, n)
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

            payload := receiveItem.udp.BaseLayer.Payload
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
    var ip any
    switch self.ipVersion {
    case 4:
        ip = layers.IPv4{
            Version: 4,
            TTL: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    case 6:
        ip = layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            NextHeader: layers.IPProtocolUDP,
        }
    }

    udp := layers.UDP{
        SrcPort: self.sourcePort,
        DstPort: self.destinationPort,
    }
    udp.SetNetworkLayerForChecksum(ip.(gopacket.NetworkLayer))

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
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
    SendMtu int
    // the window size is the max amount of packet data in memory for each sequence
    // to avoid blocking, the window size should be smaller than the channel buffer byte size (channel buffer size * mean packet size)
    WindowSize int
}


type Tcp4Buffer struct {
    ctx context.Context
    sendCallback SendPacketFunction
    tcpBufferSettings *TcpBufferSettings

    mutex sync.Mutex

    sequences map[BufferId4]*TcpSequence
}

func NewTcp4Buffer(ctx context.Context, sendCallback SendPacketFunction,
        tcpBufferSettings *TcpBufferSettings) *Tcp4Buffer {
    return &Tcp4Buffer{
        ctx: ctx,
        sendCallback: sendCallback,
        tcpBufferSettings: tcpBufferSettings,
        sequences: map[BufferId4]*TcpSequence{},
    }
}

func (self *Tcp4Buffer) receive(source Path, provideMode protocol.ProvideMode, 
        ipv4 *layers.IPv4, tcp *layers.TCP) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    bufferId := NewBufferId4(source, ipv4.SrcIP, int(tcp.SrcPort), ipv4.DstIP, int(tcp.DstPort))

    // new sequence
    if tcp.SYN {
        if sequence, ok := self.sequences[bufferId]; ok {
            sequence.Close()
            delete(self.sequences, bufferId)
        }
    }

    initSequence := func()(*TcpSequence) {
        sequence, ok := self.sequences[bufferId]
        if ok {
            return sequence
        }
        sequence = NewTcpSequence(
            self.ctx,
            self.sendCallback,
            source,
            4,
            ipv4.SrcIP,
            tcp.SrcPort,
            ipv4.DstIP,
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
    }

    if !initSequence().receive(provideMode, tcp) {
        // timed out
        delete(self.sequences, bufferId)
        initSequence().receive(provideMode, tcp)
    }
}

func (self *TcpSequence) Close() {
    self.cancel()
}


type Tcp6Buffer struct {
    ctx context.Context
    sendCallback SendPacketFunction
    tcpBufferSettings *TcpBufferSettings

    mutex sync.Mutex

    sequences map[BufferId6]*TcpSequence
}

func NewTcp6Buffer(ctx context.Context, sendCallback SendPacketFunction,
        tcpBufferSettings *TcpBufferSettings) *Tcp6Buffer {
    return &Tcp6Buffer{
        ctx: ctx,
        sendCallback: sendCallback,
        tcpBufferSettings: tcpBufferSettings,
        sequences: map[BufferId6]*TcpSequence{},
    }
}

func (self *Tcp6Buffer) receive(source Path, provideMode protocol.ProvideMode, 
        ipv6 *layers.IPv6, tcp *layers.TCP) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    bufferId := NewBufferId6(source, ipv6.SrcIP, int(tcp.SrcPort), ipv6.DstIP, int(tcp.DstPort))

    // new sequence
    if tcp.SYN {
        if sequence, ok := self.sequences[bufferId]; ok {
            sequence.Close()
            delete(self.sequences, bufferId)
        }
    }

    initSequence := func()(*TcpSequence) {
        sequence, ok := self.sequences[bufferId]
        if ok {
            return sequence
        }
        sequence = NewTcpSequence(
            self.ctx,
            self.sendCallback,
            source,
            6,
            ipv6.SrcIP,
            tcp.SrcPort,
            ipv6.DstIP,
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
    }

    if !initSequence().receive(provideMode, tcp) {
        // timed out
        delete(self.sequences, bufferId)
        initSequence().receive(provideMode, tcp)
    }
}


type TcpSequence struct {
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
    return &TcpSequence{
        ctx: cancelCtx,
        cancel: cancel,
        sendCallback: sendCallback,
        tcpBufferSettings: tcpBufferSettings,
        receiveItems: make(chan *TcpReceiveItem, tcpBufferSettings.ChannelBufferSize),
        idleCondition: NewIdleCondition(),
        ConnectionState: ConnectionState{
            source: source,
            ipVersion: ipVersion,
            sourceIp: sourceIp,
            sourcePort: sourcePort,
            destinationIp: destinationIp,
            destinationPort: destinationPort,
            // the window size starts at the fixed value
            windowSize: uint16(tcpBufferSettings.WindowSize),
        },
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

    socket, err := net.DialTimeout(
        "tcp",
        net.JoinHostPort(self.destinationIp.String(), strconv.Itoa(int(self.destinationPort))),
        self.tcpBufferSettings.ConnectTimeout,
    )
    // default is nodelay=true
    if err != nil {
        return
    }
    defer socket.Close()

    select {
    case <- self.ctx.Done():
        return
    case receiveItem := <- self.receiveItems:
        // the first packet must be a syn
        if !receiveItem.tcp.SYN {
            return
        }
        self.receiveSeq = receiveItem.tcp.Seq
        // start the send seq at 0
        // this is arbitrary, and since there is no transport security risk back to sender is fine
        self.sendSeq = 0
        packet, err := self.SynAck()
        if err != nil {
            return
        }
        self.sendCallback(self.source, packet)
    }

    go func() {
        defer self.Close()

        buffer := make([]byte, self.tcpBufferSettings.ReadBufferSize)
        
        readTimeout := time.Now().Add(self.tcpBufferSettings.ReadTimeout)
        for {
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

            if 0 < n {
                func() {
                    self.mutex.Lock()
                    defer self.mutex.Unlock()

                    for i := 0; i < n; i += self.tcpBufferSettings.SendMtu {
                        // since the transfer from local to remove is lossless and preserves order,
                        // do not worry about retransmits
                        j := min(i + self.tcpBufferSettings.SendMtu, n)
                        payload := buffer[i:j]
                        packet, err := self.DataPacket(payload)
                        if err != nil {
                            return
                        }
                        self.sendCallback(self.source, packet)
                        self.sendSeq += uint32(len(payload))
                    }

                }()
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

    for {
        select {
        case <- self.ctx.Done():
            return
        case receiveItem := <- self.receiveItems:
            if receiveItem.tcp.ACK {
                // ignore acks because we do not need to retransmit (see above)
                continue
            }

            if self.receiveSeq != receiveItem.tcp.Seq {
                // since the transfer from local to remote is lossless and preserves order,
                // this means packets were generated out of order
                // drop for now
                return
            }

            writeTimeout := time.Now().Add(self.tcpBufferSettings.WriteTimeout)

            payload := receiveItem.tcp.BaseLayer.Payload
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
                payload = payload[n:len(payload)]

                if 0 < n {
                    func() {
                        self.mutex.Lock()
                        defer self.mutex.Unlock()

                        self.receiveSeq += uint32(n)
                        packet, err := self.PureAck()
                        if err != nil {
                            return
                        }
                        self.sendCallback(self.source, packet)
                    }()
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

func (self *ConnectionState) SynAck() ([]byte, error) {
    var ip any
    switch self.ipVersion {
    case 4:
        ip = layers.IPv4{
            Version: 4,
            TTL: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    case 6:
        ip = layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            NextHeader: layers.IPProtocolUDP,
        }
    }

    tcp := layers.TCP{
        SrcPort: self.sourcePort,
        DstPort: self.destinationPort,
        Seq: self.sendSeq,
        Ack: self.receiveSeq,
        ACK: true,
        SYN: true,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip.(gopacket.NetworkLayer))

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
    var ip any
    switch self.ipVersion {
    case 4:
        ip = layers.IPv4{
            Version: 4,
            TTL: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    case 6:
        ip = layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            NextHeader: layers.IPProtocolUDP,
        }
    }

    tcp := layers.TCP{
        SrcPort: self.sourcePort,
        DstPort: self.destinationPort,
        Seq: self.sendSeq,
        Ack: self.receiveSeq,
        ACK: true,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip.(gopacket.NetworkLayer))

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
    var ip any
    switch self.ipVersion {
    case 4:
        ip = layers.IPv4{
            Version: 4,
            TTL: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    case 6:
        ip = layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            NextHeader: layers.IPProtocolUDP,
        }
    }

    tcp := layers.TCP{
        SrcPort: self.sourcePort,
        DstPort: self.destinationPort,
        Seq: self.sendSeq,
        Ack: self.receiveSeq,
        Window: self.windowSize,
    }
    tcp.SetNetworkLayerForChecksum(ip.(gopacket.NetworkLayer))

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

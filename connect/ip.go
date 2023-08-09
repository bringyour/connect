package connect


// "github.com/google/gopacket"
// "github.com/google/gopacket/layers"




// TODO as a test call RemoteUserNat directly from an Android VPN
// TODO make sure to exclude the app from the vpn by setting the network of the app to null

// TODO LocalUserNat received packet, runs security checks, and then calls callback to pass on the packet


// apply userspacenat

// buffer (processes raw packets in flight):
// 1. require tls
// 2. inspect sni. reject self signed certs. extract stream information and expose it
// 3. 



// receive from a raw socket
type ReceivePacketFunction func(source Path, provideMode ProvideMode, packet []byte)


// type AnalysisEvent 
// source Path
// - errors, transfer stats, etc


// send to a raw socket
type SendPacketFunction func(destination Path, packet []byte)



// there is no delivery requirement
// if a socket drops we just drop the packets
type RemoteUserNat struct {
    ctx context.Context
    cancel context.CancelFunc

    receiveNatPacks chan *ReceiveNatPack

    udpBufferSettings *UdpBufferSettings
    tcpBufferSettings *TcpBufferSettings

    // send callback
    sendCallbacks *CallbackList[SendPacketFunction]
}

func NewRemoteUserNat(ctx context.Context) *RemoteUserNat {
    cancelCtx, cancel := context.WithCancel(ctx)

}

// TODO provide mode of the destination determines filtering rules - e.g. local networks
// TODO currently filter all local networks and non-encrypted traffic
func (self *RemoteUserNat) ReceiveWithTimeout(source Path, provideMode ProvideMode, packet []byte, timeout time.Duration) bool {
    receiveNatPack := &ReceiveNatPack{
        Source: source,
        ProvideMode: provideMode,
        Packet: packet,
    }
    if timeout < 0 {
        self.receiveNatPacks <- receiveNatPack
        return true
    } else if 0 == timeout {
        select {
        case self.receiveNatPacks <- receiveNatPack:
            return true
        default:
            // full
            return false
        }
    } else {
        select {
        case <- self.ctx.Done():
            return false
        case self.receiveNatPacks <- receiveNatPack:
            return true
        case <- time.After(timeout):
            // full
            return false
        }
    }
}

func (self *RemoteUserNat) Receive(source Path, provideMode ProvideMode, packet []byte) {
    self.ReceiveWithTimeout(source, provideMode, packet, -1)
}

func (self *RemoteUserNat) AddSendPacketCallback(sendPacketCallback SendPacketFunction) {
    // FIXME
}

func (self *RemoteUserNat) RemoveSendPacketCallback(sendPacketCallback SendPacketFunction) {
    // FIXME
}

// SendPacketFunction
func (self *RemoteUserNat) send(destination Path, packet []byte) {
    // FIXME
}

func (self *RemoteUserNat) Run() {
    // read channel
    // parse packet
    // sort into streams, convert new stream to Socket or UDPSocket
    // on socket/udp socket response, convert back into the stream

    // each socket keyed by
    // (from path, from port, to ip, to port)
    // run send and receive on separate goroutines
    // send creates the packet state that is matched back for receive e.g. ack, etc


    udp4Buffer := NewUdp4Buffer(self.ctx, self.send)
    udp6Buffer := NewUdp6Buffer(self.ctx, self.send)
    tcp4Buffer := NewTcp4Buffer(self.ctx, self.send)
    tcp6Buffer := NewTcp6Buffer(self.ctx, self.send)


    // send
    // parse packet for protocol, src ip, src port, dest ip, dest port
    // if udp, look for active socket and send
    // if tcp, look at syn. look at sequence id and send if head, or drop. does not implement sack or window size options because the transport is lossless. the only way we get out of order is if the sender is out of order


    // parse version
    // parse ipv4 (source ip, dest ip, protocol)
    //   if tcp, parse (src port, dest port, syn, sequence num)
    //       hand to channel, then send ack (or syn+ack if the packet was a syn)
    //   if udp, parse (src port, dest port)
    //       hand to channel


    // version := uint8(data[0]) >> 4
    // 4 or 6
    // layers.ipv4.DecodeFromBytes

    // BaseLayer.Payload

    // layers.udp.DecodeFromBytes
    // layers.tcp.DecodeFromBytes

    for {
        select {
        case <- self.ctx.Done():
            return
        case receiveNatPack := <- self.receiveNatPacks:
            ipPacket := receiveNatPack.packet
            ipVersion := uint8(ipPacket[0]) >> 4
            switch ipVersion {
            case 4:
                ipv4 := layers.IPv4{}
                ipv4.DecodeFromBytes(ipPacket)
                switch ipv4.Protocol {
                case layers.IPProtocolUDP:
                    udp := layers.UDP{}
                    udp.DecodeFromBytes(ipv4.BaseLayer.Payload)

                    udp4Buffer.receive(
                        receiveNatPack.source,
                        receiveNatPack.provideMode,
                        ipv4,
                        udp
                    )
                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv4.BaseLayer.Payload)

                    tcp4Buffer.receive(
                        receiveNatPack.source,
                        receiveNatPack.provideMode,
                        ipv4,
                        tcp,
                        tcp.BaseLayer.Payload,
                    )
                default:
                    // no support for this protocol, drop
                }
            case 6:
                ipv6 := layers.IPv6{}
                ipv6.DecodeFromBytes(ipPacket)
                switch ipv6.Protocol {
                case layers.IPProtocolUDP:
                    udp := layers.UDP{}
                    udp.DecodeFromBytes(ipv6.BaseLayer.Payload)

                    udp6Buffer.receive(
                        receiveNatPack.source,
                        receiveNatPack.provideMode,
                        ipv6,
                        udp
                    )
                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv6.BaseLayer.Payload)

                    tcp6Buffer.receive(
                        receiveNatPack.source,
                        receiveNatPack.provideMode,
                        ipv6,
                        tcp,
                        tcp.BaseLayer.Payload,
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


type ReceiveNatPack struct {
    Source Path
    ProvideMode ProvideMode
    Packet []byte
}



// FIXME must implement comparable
type BufferId struct {
    source Path
    sourceIp net.IP
    sourcePort int
    destinationIp net.IP
    destinationPort int
}

func NewBufferIdFromIpv4(source Path, ipv4 *layers.IPv4) {

}

func NewBufferIdFromIpv6(source Path, ipv6 *layers.IPv6) {

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



type UdpBuffer struct {
    ctx context.Context
    sendCallback SendPacketFunction

    sequences map[Udp4StreamId]*UdpSequence
}

func NewUpd4Buffer(ctx context.Context, sendCallback SendPacketFunction) *Udp4Buffer {
    return &Udp4Buffer{
        ctx: ctx,
        sendCallback: sendCallback,
        sequences: map[Udp4StreamId]*UdpSequence{}
    }
}

func (self *Udp4Buffer) receive4(source Path, provideMode ProvideMode 
        ipv4 *layers.IPv4, udp *layers.UDP) {
}

func (self *Udp4Buffer) receive6(source Path, provideMode ProvideMode 
        ipv6 *layers.IPv6, udp *layers.UDP) {
}

func (self *Udp4Buffer) receive(source Path, provideMode ProvideMode 
        bufferId *BufferId, udp *layers.UDP) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    initSequence := func()(*UdpSequence) {
        sequence, ok := self.sequences[streamId]
        if ok {
            return sequence
        }
        sequence = NewUdpSequence(
            self.ctx,
            self.sendCallback,
            4,
            ipv4.SourceIp,
            udp.SourcePort,
            ipv4.DestinationIp,
            udp.DestionationPort,
        )
        self.sequences[streamId] = sequence
        go func() {
            sequence.Run()

            self.mutex.Lock()
            defer self.mutex.Unlock()
            // clean up
            if sequence == self.sequences[streamId] {
                delete(self.sequences, streamId)
            }
        }()
        return sequence
    }

    if !initSequence().receive(source, provideMode, ipv4, udp) {
        // sequence closed
        delete(self.sequences, streamId)
        initSequence().receive(source, provideMode, ipv4, udp)
    }
}


type UdpSequence struct {
    ctx context.Context
    cancel context.CancelFunc
    sendCallback SendPacketFunction

    receiveItems chan *UpdReceiveItem

    StreamState
}

func NewUdpSequence(ctx context.Context, sendCallback SendPacketFunction,
        ipVersion int,
        sourceIp net.IP, sourcePort uint16,
        destinationIp net.IP, destinationPort uint16,
        udpBufferSettings *UdpBufferSettings) *Udp4Sequence {
    cancelCtx, cancel := context.WithCancel(ctx)
    return &UdpSequence{
        ctx: cancelCtx,
        cancel: cancel,
        sendCallback: sendCallback,
        receiveItems: make(chan *UpdReceiveItem, udpBufferSettings.BufferSize),
        StreamState: StreamState{
            ipVersion: ipVersion,
            sourceIp: sourceIp,
            sourcePort: sourcePort,
            destinationIp: destinationIp,
            destinationPort: destinationPort
        },
    }
}

func (self *Udp4Sequence) receive(source *Path, provideMode ProvideMode, udp *layers.UDP) (success bool) {
    if !self.idleCondition.updateOpen() {
        success = false
        return
    }
    defer self.idleCondition.updateClose()
    defer func() {
        // this means there was some error in sequence processing
        if err := recover(); err != nil {
            success = false
        }
    }()
    self.receiveItems <- &ReceiveItem{
        source: source,
        provideMode: provideMode,
        udp: udp,
    }
    success = true
    return
}

func (self *Udp4Sequence) Run() {
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

    socket, err := new.Dial("udp", net.JoinHostPort(self.destinationIp.String(), self.destinationPort))
    if err != nil {
        return
    }
    defer socket.Close()

    go func() {
        defer self.Close()
        buffer := make([]byte, self.udpBufferSettings.ReceiveBufferSize)

        readTimeout = time.Now() + self.udpBufferSettings.ReadTimeout
        for {
            select {
            case <- ctx.Done():
                return
            default:
            }

            deadline := Min(time.Now() + self.udpBufferSettings.ReadPollTimeout, readTimeout)
            socket.SetReadDeadline(deadline)
            n, err := socket.Read()

            if 0 < n {
                for i := 0; i < n; i += self.udpBufferSettings.SendMtu {
                    j := Min(i + self.udpBufferSettings.SendMtu, n)
                    packet := self.DataPacket(buffer[i:j])
                    self.sendCallback(self.source, packet)
                }
                readTimeout = time.Now() + self.udpBufferSettings.ReadTimeout
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
        checkpointId := self.idleCondition.checkpoint()
        select {
        case <- self.ctx.Done():
            return
        case receiveItem := <- self.updReceiveItems:
            writeTimeout = time.Now() + self.udpBufferSettings.WriteTimeout

            payload := receiveItem.udp.BaseLayer.Payload
            for 0 < len(payload) {
                select {
                case <- ctx.Done():
                    return
                default:
                }

                deadline := Min(time.Now() + self.udpBufferSettings.WritePollTimeout, writeTimeout)
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
        case time.After(self.udpBufferSettings.IdleTimeout):
            if self.idleCondition.close(checkpointId) {
                // close the sequence
                return
            }
            // else there pending updates
        }
    }
}

type UpdReceiveItem struct {
    source Path
    provideMode ProvideMode
    udp *layers.UDP
}

type StreamState struct {
    ipVersion int
    sourceIp net.IP
    sourcePort uint16,
    destinationIp net.IP
    destinationPort uint16
}
func (self *StreamState) DataPacket(payload []byte) []byte {
    var ip gopacket.NetworkLayer
    switch self.ipVersion {
    case 4:
        ip := layers.IPv4{
            Version: 4,
            TTL: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    case 6:
        ip := layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    }

    udp := layers.UDP{
        SrcPort: self.sourcePort,
        DstPort: self.destinationPort,
    }
    udp.SetNetworkLayerForChecksum(&ip)

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
    }

    buffer := gopacket.NewSerializeBuffer()

    err := gopacket.SerializeLayers(buffer, options,
        &ip,
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
    ConnectTimeout int
    ReadTimeout int
    WriteTimeout int
    ReadBufferSize int
    ChannelBufferSize int
    SendMtu int
    // the window size should be smaller than the channel buffer byte size (channel buffer size * mean packet size)
    WindowSize int
}


// source ip, source port, dest ip, dest port
type Tcp4ConnectionId = byte[12]

type Tcp4Buffer struct {
    ctx
    sendCallback SendPacketFunction
}

func (self *Tcp4Buffer) receive(receiveNatPack *ReceiveNatPack, 
        ipv4 *layers.IPv4, tcp *layers.TCP, payload []byte) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    connectionId := Tcp4ConnectionId{}
    connectionId[0:4] = ipv4.SourceIp[0:4]
    connectionId[5] = byte(ipv4.SourcePort >>> 8)
    connectionId[6] = byte(ipv4.SourcePort)
    connectionId[7:11] = ipv4.DestinationIp[0:4]
    connectionId[12] = byte(ipv4.DestinationPort >>> 8)
    connectionId[13] = byte(ipv4.DestinationPort)

    // new sequence
    if SYN {
        if sequence, ok := self.sequences[connectionId]; ok {
            sequences.Close()
            delete(self.sequences, connectionId)
        }
    }

    initSequence := func()(*ReceiveSequence) {
        sequence, ok := self.sequences[connectionId]
        if ok {
            return sequence
        }
        sequence = NewTcp4Sequence(
            
        )
        self.sequence[connectionId] = sequence
        go func() {
            sequence.Run()

            self.mutex.Lock()
            defer self.mutex.Unlock()
            // clean up
            if sequence == self.sequences[connectionId] {
                delete(self.sequences, connectionId)
            }
        }()
        return receiveSequence
    }

    if !initSequence().receive(XX) {
        // timed out
        delete(self.sequences, connectionId)
        initSequence().receive(XXX)
    }
    
}









// provideMode, ipv4 *layers.IPv4, tcp *layers.TCP, payload []byte

type TcpSequence struct {
    ctx
    cancel
    
    sendCallback SendPacketFunction

    receiveItems chan *TcpReceiveItem

    ConnectionState
}

// on recv syn, send syn+ack
// on recv, send ack
// send must form the seq correctly

func (self *Tcp4Sequence) receive(source *Path, provideMode ProvideMode, tcp *layers.TCP) {
    if !self.idleCondition.updateOpen() {
        success = false
        return
    }
    defer self.idleCondition.updateClose()
    defer func() {
        // this means there was some error in sequence processing
        if err := recover(); err != nil {
            success = false
        }
    }()
    self.receiveItems <- &TcpReceiveItem{
        source: source,
        provideMode: provideMode,
        tcp: tcp,
    }
    success = true
    return
}

func Run() {
    // read first
    // if should be syn
    // respond with syn+ack
    // open socket
    // go read()
    // read, check seq, write to socket

    defer func() {
        self.Close()

        close(self.receiveItems)

        // drain and drop
        func() {
            for {
                select {
                case _, ok := <- self.receiveItems {
                    if !ok {
                        return
                    }
                }
            }
        }()
    }()

    socket, err := new.DialTimeout(
        "tcp",
        net.JoinHostPort(self.destinationIp.String(), self.destinationPort),
        self.tcpBufferSettings.ConnectTimeout,
    )
    if err != nil {
        return
    }
    defer socket.Close()

    socker.SetNoDelay(true)

    select {
    case <- self.ctx.Done():
        return
    case receiveItem := <- self.receiveItems:
        // the first packet must be a syn
        if !receiveItem.tcp.Syn {
            return
        }
        self.receiveSeq = receiveItem.tcp.Sequence
        // start the send seq at 0
        // this is arbitrary, and since there is no transport security risk back to sender is fine
        self.sendSeq = 0
        packet := self.SynAck()
        self.sendCallback(packet)
    }

    // read
    go func() {
        defer self.Close()

        buffer := make([]byte, self.tcpBufferSettings.ReceiveBufferSize)
        
        readTimeout := time.Now() + self.tcpBufferSettings.ReadTimeout
        for {
            select {
            case <- ctx.Done():
                return
            default:
            }

            deadline := Min(readTimeout, time.Now() + self.tcpBufferSettings.ReadPollTimeout)
            socket.SetReadDeadline(deadline)
            
            n, err := socket.Read(buffer)

            if 0 < n {
                func() {
                    self.mutex.Lock()
                    defer self.mutex.Unlock()

                    for i := 0; i < n; i += self.tcpBufferSettings.SendMtu {
                        // since the transfer from local to remove is lossless and preserves order,
                        // do not worry about retransmits
                        j := Min(i + self.tcpBufferSettings.SendMtu, n)
                        payload := buffer[i:j]
                        packet := self.DataPacket(payload)
                        self.sendCallback(packet)
                        self.sendSeq += len(payload)
                    }

                }()
                readTimeout = time.Now() + self.tcpBufferSettings.ReadTimeout
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
            if receiveItem.tcp.Sequence.Ack {
                // ignore acks because we do not need to retransmit (see above)
                continue
            }

            if self.receiveSeq != receiveItem.tcp.Sequence {
                // since the transfer from local to remote is lossless and preserves order,
                // this means packets were generated out of order
                // drop for now
                return
            }

            writeTimeout := time.Now() + self.tcpBufferSettings.WriteTimeout

            payload := receiveItem.tcp.BaseLayer.Payload
            for 0 < len(payload) {
                select {
                case <- ctx.Done():
                    return
                default:
                }

                deadline := Min(writeTimeout, time.Now() + self.tcpBufferSettings.WritePollTimeout)
                socket.SetWriteDeadline(deadline)
                n, err := socket.WRITE(payload)
                payload = payload[n:len(payload)]

                if 0 < n {
                    func() {
                        self.mutex.Lock()
                        defer self.mutex.Unlock()

                        self.receiveSeq += n
                        packet := self.PureAck()
                        self.sendCallback(packet)
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
    source Path
    provideMode ProvideMode
    tcp *layers.Tcp
}

type ConnectionState struct {
    ipVersion int
    sourceIp net.IP
    sourcePort uint16,
    destinationIp net.IP
    destinationPort uint16


    mutex sync.Mutex

    receiveSeq uint32
    sendSeq uint32

    windowSize int

}

func (self *ConnectionState) SynAck() []byte {
    var ip gopacket.NetworkLayer
    switch self.ipVersion {
    case 4:
        ip := layers.IPv4{
            Version: 4,
            TTL: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    case 6:
        ip := layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    }

    udp := layers.TCP{
        SrcPort: self.sourcePort,
        DstPort: self.destinationPort,
        Sequence: self.sendSeq,
        AckSequence: self.receiveSeq,
        Ack: true,
        Syn: true,
        Window: self.WindowSize,
    }
    udp.SetNetworkLayerForChecksum(&ip)

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
    }

    buffer := gopacket.NewSerializeBuffer()

    err := gopacket.SerializeLayers(buffer, options,
        &ip,
        &udp,
    )

    if err != nil {
        return nil, err
    }
    packet := buffer.Bytes()
    return packet, nil
}

func (self *ConnectionState) PureAck() []byte {
    var ip gopacket.NetworkLayer
    switch self.ipVersion {
    case 4:
        ip := layers.IPv4{
            Version: 4,
            TTL: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    case 6:
        ip := layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    }

    udp := layers.TCP{
        SrcPort: self.sourcePort,
        DstPort: self.destinationPort,
        Sequence: self.sendSeq,
        AckSequence: self.receiveSeq,
        Ack: true,
        Window: self.WindowSize,
    }
    udp.SetNetworkLayerForChecksum(&ip)

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
    }

    buffer := gopacket.NewSerializeBuffer()

    err := gopacket.SerializeLayers(buffer, options,
        &ip,
        &udp,
    )

    if err != nil {
        return nil, err
    }
    packet := buffer.Bytes()
    return packet, nil
}

func (self *ConnectionState) DataPacket(payload []byte) []byte {
    var ip gopacket.NetworkLayer
    switch self.ipVersion {
    case 4:
        ip := layers.IPv4{
            Version: 4,
            TTL: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    case 6:
        ip := layers.IPv6{
            Version: 6,
            HopLimit: 64,
            SrcIP: self.sourceIp,
            DstIP: self.destinationIp,
            Protocol: layers.IPProtocolUDP,
        }
    }

    udp := layers.TCP{
        SrcPort: self.sourcePort,
        DstPort: self.destinationPort,
        Sequence: self.sendSeq,
        AckSequence: self.receiveSeq,
        Ack: true,
        Window: self.WindowSize,
    }
    udp.SetNetworkLayerForChecksum(&ip)

    options := gopacket.SerializeOptions{
        ComputeChecksums: true,
        FixLengths: true,
    }

    buffer := gopacket.NewSerializeBuffer()

    err := gopacket.SerializeLayers(buffer, options,
        &ip,
        &udp,
        gopacket.Payload(payload),
    )

    if err != nil {
        return nil, err
    }
    packet := buffer.Bytes()
    return packet, nil
}




// FIXME LocalUserNat applies analytics like SNI and data per host, then forms a message to the RemoteUserNat to egress





// FIXME remote user nat SendPacket takes a ProvideMode
// FIXME options to reject local networks



/*


// form the IP frame type
// listen for IP frame type with response=true
// ip frame should have a response bool
type LocalPacketNat struct {
    // destinationIds []ulid.ULID

}


// receive the IP frame and process, sending frames back
type RemotePacketNat struct {

}


*/




/*

unat

local
receive local -> forward to remote
receive remove -> forward to local

remote
receive remote ->
  if ack, ignore 
  // the ack rate is limited by the send rate
  form local socket, send data 
    generate ack -> remote

receive local ->
  form to tcp packet and sequence
  send response packet
  // because reliable transmit, we don't need to worry about receiving acks

*/

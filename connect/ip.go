package connect


// "github.com/google/gopacket"
// "github.com/google/gopacket/layers"


// apply userspacenat

// buffer (processes raw packets in flight):
// 1. require tls
// 2. inspect sni. reject self signed certs. extract stream information and expose it
// 3. 



// receive from a raw socket
type ReceivePacketFunction func(source Path, provideMode ProvideMode, packet []byte)


// send to a raw socket
type SendPacketFunction func(destination Path, packet []byte)



// there is no delivery requirement
// if a socket drops we just drop the packets
type RemoteUserNat struct {
    ctx context.Context
    cancel context.CancelFunc

    receiveNatPacks chan *ReceiveNatPack

    // send callback
}

// TODO provide mode of the destination determines filtering rules - e.g. local networks
// TODO currently filter all local networks and non-encrypted traffic
func (self *RemoteUserNat) Receive(source Path, provideMode ProvideMode, packet []byte) {
}

func (self *RemoteUserNat) AddSendPacketCallback(sendPacketCallback SendPacketFunction) {

}

func (self *RemoteUserNat) RemoveSendPacketCallback(sendPacketCallback SendPacketFunction) {

}

// SendPacketFunction
func (self *RemoteUserNat) send(destination Path, packet []byte) {

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


    udp4Buffer := NewUdp4Buffer(self.ctx)
    tcp4Buffer := NewTcp4Buffer(self.ctx)
    // TODO udp6, tcp6


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

                    udpBuffer.receive4(
                        receiveNatPack.source,
                        receiveNatPack.provideMode,
                        ipv4,
                        tcp,
                        udp.DestionationPort,
                        udp.BaseLayer.Payload,
                    )

                case layers.IPProtocolTCP:
                    tcp := layers.TCP{}
                    tcp.DecodeFromBytes(ipv4.BaseLayer.Payload)

                    tcpBuffer.receive4(
                        receiveNatPack.source,
                        receiveNatPack.provideMode,
                        ipv4,
                        tcp,
                        udp.BaseLayer.Payload,
                    )
                default:
                    // no support for this protocol, drop
                }
            case 6:
                // TODO
                // drop for now
            }
        }
    }


}

func (self *RemoteUserNat) Close() {
    self.cancel()
}


type Udp4Buffer struct {
    ctx
    sendCallback SendPacketFunction
}


func (self *Udp4Buffer) receive(receiveNatPack *ReceiveNatPack, 
        ipv4 *layers.IPv4, udp *layers.UDP, payload []byte) {

}



type Udp4Sequence struct {
    ctx
    cancel
    sendCallback SendPacketFunction
}




type Tcp4Buffer struct {
    ctx
    sendCallback SendPacketFunction
}

func (self *Tcp4Buffer) receive(receiveNatPack *ReceiveNatPack, 
        ipv4 *layers.IPv4, tcp *layers.TCP, payload []byte) {
    
}


// provideMode, ipv4 *layers.IPv4, tcp *layers.TCP, payload []byte

type Tcp4Sequence struct {
    ctx
    cancel
    sendCallback SendPacketFunction

    source Path

    tcp4ReceiveNatPacks
}

// on recv syn, send syn+ack
// on recv, send ack
// send must form the seq correctly

func (self *Tcp4Sequence) receive() {

}

func Run() {
    // read first
    // if should be syn
    // respond with syn+ack
    // open socket
    // go read()
    // read, check seq, write to socket

    var socket Socket

    defer func() {
        self.Close()

        close(self.tcp4ReceiveNatPacks)

        // close socket
        if socket != nil {
            socket.Close()
        }

        // just drop all pending packets in `tcp4ReceiveNatPacks`


    }()

    var receiveSeq uint32

    var sendSeq uint32

    select {
    case <- self.ctx.Done():
        return
    case tcp4ReceiveNatPack := <- self.tcp4ReceiveNatPacks:
        if !SYN {
            return
        }
        seq = SEQ
        send(SYN_ACK)
    }

    socket = OPEN_SOCKET()
    // read
    go func() {
        // we don't need to follow an MTU since this is sent directly into the local raw socket
        buffer [4096]byte
        
        for {
            select {
            case ctx.Done():
                return
            default:
            }
            socket.SetReadDeadline(now + ONE_SECOND)
            socket.Read()
            if TIMEOUT {
                continue
            }

            // create tcp packet incrementing sendSeq
            send(PACKET)
        }
    }()

    for {
        select {
        case <- self.ctx.Done():
            return
        case tcp4ReceiveNatPack := <- self.tcp4ReceiveNatPacks:
            // check sequence number against receiveSeq
            if !INORDER {
                // since the transfer from local to remote is lossless and preserves order,
                // this means packets were generated out of order
                // TODO drop for now
                return
            }

            for {
                select {
                case ctx.Done():
                    return
                default:
                }
                socket.SetWriteDeadline(now + ONE_SECOND)
                socket.WRITE(PAYLOAD)
                if TIMEOUT {
                    continue
                }
                break
            }
        }
    }



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

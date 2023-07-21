package connect


// apply userspacenat

// buffer (processes raw packets in flight):
// 1. require tls
// 2. inspect sni. reject self signed certs. extract stream information and expose it
// 3. 


// there is no delivery requirement
// if a socket drops we just drop the packets
type UserNat struct {
	// receive callback
}

func SendIpPacket(ipPacket IpPacket) {

}

func SetIpPacketCallback() {

}

func Run() {
	// read channel
	// parse packet
	// sort into streams, convert new stream to Socket or UDPSocket
	// on socket/udp socket response, convert back into the stream

	// each socket keyed by
	// (from path, from port, to ip, to port)
	// run send and receive on separate goroutines
	// send creates the packet state that is matched back for receive e.g. ack, etc

}



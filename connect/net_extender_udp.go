package connect



// this extender mode enables small mtu udp packets
// e.g. these can fit within a dns allowance


// wraps a PacketConn as a streaming Conn with reliable delivery


type ServerUdpConn struct {

	serverPacketConn net.PacketConn
	connect chan net.Conn 
}

func NewServerUdpConn() *ServerUdpConn {
	serverConn := &ServerUdpConn{

	}
	go serverConn.run()
	return serverConn
}

func (self *ServerUdpConn) Listen() (net.Conn, error) {
	select {
	case <- self.ctx.Done():
		return nil, errors.New("Done.")
	case conn := <- self.connect:
		return conn, nil
	}
}

func (self *ServerUdpConn) run() {
	defer self.Close()

	connMutex sync.Mutex
	conns := map[net.Addr]*BaseUdpConn{}

	buffer := make([]byte, 4096)
	for {
		n, addr, err := self.serverPacketConn.ReadFrom(buffer)
		if err != nil {
			return
		}
		packetBytes := slices.Clone(buffer[0:n])

		connMutex.Lock()
		conn, ok := conns[addr]
		connMutex.Unlock()
		if ok {
			success, err := conn.Add(packetBytes)
			ok = success && err == nil
		}
		if !ok {
			conn = &BaseUdpConn{

			}
			connMutex.Lock()
			conns[addr] = conn
			connMutex.Unlock()
			go func() {
				select {
				case <- conn.transport.Done():
				}
				connMutex.Lock()
				defer connMutex.Unlock()
				if conn == conns[addr] {
					delete(conns, addr)
				}
			}()
			go func() {
				defer conn.Close()
				for {
					select {
					case <- conn.transport.Done():
						return
					case sendPacketBytes <- conn.transport.packetSend:
						err := self.serverPacketConn.WriteTo(sendPacketBytes, addr)
						if err != nil {
							return
						}
					}
				}
			}()
			conn.Add(packetBytes)
		}
	}
}

// FIXME when detect a new source addr, create a new connection for that addr
// each new conn waits for for transport.Done() and removes when done






type ClientUdpConn struct {
	packetConn net.Conn
	BaseUdpConn
}

func NewClientUdpConn(packetConn net.Conn) *ClientUdpConn {
	clientConn := &ClientUdpConn{}
	go clientConn.run()
	return clientConn
}

func (self *ClientUdpConn) run() {
	defer self.packetConn.Close()

	go func() {
		defer Close()

		buffer := make([]byte, 4096)
		for {
			select {
			case <- self.ctx.Done():
				return
			case <- self.transport.Done():
				return
			default:
			}

			n, err := self.packetConn.Read(buffer)
			if err != nil {
				return
			}
			packetBytes := COPY(buffer[0:n])
			success, err := transport.AddPacket(packetBytes)
			if err != nil {
				return
			}
			if !success {
				return
			}
		}
	}()

	for {
		select {
		case <- self.ctx.Done():
			return
		case <- self.transport.Done():
			return
		case packetBytes := self.transport.packetReceive:
			n, err := self.packetConn.Write(packetBytes)
			if err != nil {
				return
			}
			if len(packetBytes) != n {
				return
			}
		}
	}
}



type BaseUdpConn struct {
	ctx context.Context
	cancel context.CancelFunc

	transport *SimpleReliableUdpTransport
	writeDeadline time.Time
	readDeadline time.Time

	readBuffer []byte
}

// net.Conn implementation

func (self *SimpleReliableUdpConn) Write(b []byte) (int, error) {
	timeout := time.Now().Sub(self.writeDeadline)
	if timeout < 0 {
		select {
		case <- self.ctx.Done():
			return 0, errors.New("Done.")
		case <- self.transport.Done():
			return 0, errors.New("Done.")
		case self.transport.send <- b:
			return len(b), nil
		}
	} else {
		select {
		case <- self.ctx.Done():
			return 0, errors.New("Done.")
		case <- self.transport.Done():
			return 0, errors.New("Done.")
		case self.transport.send <- b:
			return len(b), nil
		case <- time.After(timeout):
			return 0, errors.New("Timeout.")
		}
	}
}

func (self *SimpleReliableUdpConn) Read(b []byte) (int, error) {
    m := min(len(self.buffer), len(b))
    if 0 < m {
    	copy(b[0:m], self.buffer[0:m])
    	self.buffer = self.buffer[m:]
    }
    if len(b) == m {
    	return m, nil
    }
    
    // blocking read into the buffer
    timeout := time.Now().Sub(self.readDeadline)
	if timeout < 0 {
		select {
		case <- self.ctx.Done():
			return m, errors.New("Done.")
		case <- self.transport.Done():
			return m, errors.New("Done.")
		case messageBytes, ok <- self.transport.receive:
			if !ok {
				return m, nil
			}
			self.buffer = append(self.buffer, messageBytes...)
		}
	} else {
		select {
		case <- self.ctx.Done():
			return m, errors.New("Done.")
		case <- self.transport.Done():
			return m, errors.New("Done.")
		case messageBytes, ok <- self.transport.receive:
			if !ok {
				return m, nil
			}
			self.buffer = append(self.buffer, messageBytes...)
		case <- time.After(timeout):
			return m, errors.New("Timeout.")
		}
	}

	// non blocking read more into the buffer
    Fill:
    for m < len(b) {
    	n := min(len(self.buffer), len(b) - m)
		if 0 < n {
	    	copy(b[m:m+n], self.buffer[0:n])
	    	self.buffer = self.buffer[n:]
	    	m += n
	    }

    	select {
		case <- self.ctx.Done():
			return m, errors.New("Done.")
		case <- self.transport.Done():
			return m, errors.New("Done.")
		case messageBytes, ok <- self.transport.receive:
			if !ok {
				return m, nil
			}
			self.buffer = append(self.buffer, messageBytes...)
		default:
			break Fill
		}
    }

    return m, nil
}

func (self *SimpleReliableUdpConn) Close() error {
    self.cancel()
    self.transport.Close()
    return nil
}

func (self *SimpleReliableUdpConn) LocalAddr() net.Addr {
    return self.transport.LocalAddr()
}

func (self *SimpleReliableUdpConn) RemoteAddr() net.Addr {
    return self.transport.RemoteAddr()
}

func (self *SimpleReliableUdpConn) SetDeadline(t time.Time) error {
    self.readDeadline = t
    self.writeDeadline = t
    return nil
}

func (self *SimpleReliableUdpConn) SetReadDeadline(t time.Time) error {
    self.readDeadline = t
    return nil
}

func (self *SimpleReliableUdpConn) SetWriteDeadline(t time.Time) error {
    self.writeDeadline = t
    return nil
}







type SimpleReliableUdpTransportConfig struct {
	Mtu int
	MaxSendBufferByteCount ByteCount
	MaxReceiveBufferByteCount ByteCount
	RetransmitTimeout time.Duration
	IdleTimeout time.Duration
}


type SimpleReliableUdpTransport struct {
	// packet conn
	// ackNumber
	// ackMonitor

	packetSend chan []byte
	packetReceive chan []byte
	send chan []byte
	receive chan []byte

	ackMutex *sync.Mutex
	ackNumber int
	ackMonitor *Monitor
}

func (self *SimpleReliableUdpTransport) AddPacket(packetBytes []byte) (boolean, error) {
	// check for idle timeout

	// FIXME
	// packetReceive <- packetBytes

	select {
	case <- self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	if !self.idleCondition.UpdateOpen() {
		return false, errors.New("Done.")
	}
	defer self.idleCondition.UpdateClose()

	select {
	case <- self.ctx.Done():
		return false, errors.New("Done.")
	case packetReceive <- packetBytes:
		return true, nil
	}
}

func (self *SimpleReliableUdpTransport) receive() {
	receiveNumber := 0
	queue := NEW

	for {
		var packetBytes []byte
		select {
		case <- self.ctx.Done():
			return
		case packetBytes = <- self.packetReceive:
		case <- time.After(timeout):
			if 0 == self.resendQueue.Len() {
				// idle timeout
				if self.idleCondition.Close(checkpointId) {
					// close the sequence
					return
				}
				// else there are pending updates
			}
		}

		frame := &protocol.Frame{}
		err := protobuf.Unmarshal(buffer[0:n], &frame)
		if err != nil {
			// skip the packet
			continue
		}

		switch frame.MessageType {
		case ExtenderUdpPack:

			if NUMBER < receiveNumber {
				// already received
				SEND_ACK(receiveNumber - 1)
			} else {

				receiveQueue.Add(messageBytes)

				receiveCount := 0
				for queue.PeekFirst().SequenceNumber() == receiveNumber {
					receiveCount += 1
					receiveNumber += 1
					messageBytes := REMOVE()
					select {
					case self.ctx.Done():
						return
					case self.receive <- messageBytes:
					}
				}
				if 0 <= receiveCount {
					SEND_ACK(receiveNumber - 1)
				}
				for {
					_, queueByteCount := queue.QueueSize()
					if queueByteCount <= config.MaxReceiveBufferByteCount {
						break
					}
					queue.RemoveLast()
				}
			}
		case ExtenderUdpAck:
			func() {
				self.ackMutex.Lock()
				defer self.ackMutex.Unlock()

				self.ackNumber = NUMBER
				self.ackMonitor.NotifyAllInLock()
			}()
			
		}

		


		// parse ExtenderUdpPack or ExtenderUdpAck
		// if ack, remove all items in the resend queue up to ack

		// if pack, add to receive queue
		// if head, receive immediately. send ack
		//    process all receive queue heads
		// if no more space in receive queue, remove highest indexes until there is space
		// add to receive queue
	}
}

func (self *SimpleReliableUdpTransport) send() {
	sendNumber := 0
	queue := NEW

	for {
		ack := self.ackMonitor.NotifyChannel()
		// resend items

		sendTime := time.Now()
		for resendQueue.Fist().sendTime.Before(sendTime) {

			packetBytes := resendQueue.RemoveFirst()
			select {
			case self.ctx.Done():
				return
			case self.packetSend <- packetBytes:
				ADD_RESEND_QUEUE(packetBytes)
			}
		}



		if _, queueByteCount := queue.QueueSize(); config.MaxSendBufferByteCount <= queueByteCount {
			// listen to ack monitor

			select {
			case <- self.ctx.Done():
				return
			case <- ack:
			case <- time.After(resendTimeout):
			case <- time.After(timeout):
				if 0 == self.resendQueue.Len() {
					// idle timeout
					if self.idleCondition.Close(checkpointId) {
						// close the sequence
						return
					}
					// else there are pending updates
				}
			}

		} else {
			select {
			case <- self.ctx.Done():
				return
			case <- ack:
			case messageBytes <- send:
				// fragment by mtu, send, and add to resend queue

				for i := 0; i < len(messageBytes); i += self.config.Mtu {
					j := min(i + self.config.Mtu, len(messageBytes))
					messageFragmentBytes := slices.Clone(messageBytes[i:j])
					packetBytes := proto.Marshal(FRAME)
					select {
					case self.ctx.Done():
						return
					case self.packetSend <- packetBytes:
						ADD_RESEND_QUEUE(packetBytes)
					}
				}

			case <- time.After(resendTimeout):
			case <- time.After(timeout):
				if 0 == self.resendQueue.Len() {
					// idle timeout
					if self.idleCondition.Close(checkpointId) {
						// close the sequence
						return
					}
					// else there are pending updates
				}
			}
		}
	}
}


type udpReceiveItem struct {
	transferItem

}

// ordered by sequenceNumber
type receiveQueue = transferQueue[*udpReceiveItem]

func newReceiveQueue() *receiveQueue {
	return newTransferQueue[*udpReceiveItem](func(a *udpReceiveItem, b *udpReceiveItem)(int) {
		if a.sequenceNumber < b.sequenceNumber {
			return -1
		} else if b.sequenceNumber < a.sequenceNumber {
			return 1
		} else {
			return 0
		}
	})
}


type udpSendItem struct {
	transferItem

	sendTime time.Time
	resendTime time.Time
	
}

type udpResendQueue = transferQueue[*udpSendItem]

func newUdpResendQueue() *udpResendQueue {
	return newTransferQueue[*udpSendItem](func(a *udpSendItem, b *udpSendItem)(int) {
		if a.resendTime.Before(b.resendTime) {
			return -1
		} else if b.resendTime.Before(a.resendTime) {
			return 1
		} else {
			return 0
		}
	})
}



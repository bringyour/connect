package



type ExtenderUdpServer struct {

	// listens on 53
	// each session, read and print mbps send and receive, send data as fast as possible


}

func NewServer() {









}

func run() {

	conn := net.ListenUDP()
	defer conn.Close()

	for {
		n, addr, err := conn.ReadFromUDP(buffer)

		var sequence *clientSequence
		func() {
			LOCK
			defer UNLOCK

			sequences[addr]
		}

		sequence.Receive(pack)



	}
}



type clientSequence struct {
	ctx context.Context
	cancel context.CancelFunc
	conn UDPConn
	addr UDPAddr

	receivePacks chan []byte
}

func Receive(pack []byte) {
	select {
	case <- ctx.Done():
		return
	case receivePacks <- pack:
	}
}

func run() {
	defer cancel()

	nextSend := time.Time{}
	for {
		timeout := nextSend.Sub(time.Now())
		if timeout <= 0 {
			// send

			nextSend = time.Now().Add(SendTimeout)
			timeout = SendTimeout
		}

		select {
		case <- self.ctx.Done():
			return
		case pack := <- self.receivePacks:

			// read payload
			// receive count is first 8 bytes
			// rest is data
			// update receive data
		case <- time.After(sendTimeout):
		}
	}
}






type ExtenderUdpClient struct {

	// connects to 53
	// read and print mbps send and receive, send data as fast as possible

	ctx context.Context
	cancel context.CancelFunc
	conn UDPConn

	
	receivePacks chan []byte

}

func run() {

	go func() {

		for {
			n, err := conn.Read(buffer)

			select {
			case <- ctx.Done():
				return
			case receivePacks <- pack:
			}
		}
	}()

	nextSend := time.Time{}
	for {
		timeout := nextSend.Sub(time.Now())
		if timeout <= 0 {
			// send

			nextSend = time.Now().Add(SendTimeout)
			timeout = SendTimeout
		}

		select {
		case <- self.ctx.Done():
			return
		case pack := <- self.receivePacks:

			// read payload
			// receive count is first 8 bytes
			// rest is data
			// update receive data
		case <- time.After(sendTimeout):
		}
	}
}






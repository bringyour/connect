package connect




// multi hop buffer
// buffers until for the StreamOpen message
// or timeout, then close

// if the buffer closes, it cannot re-open in the future. that would break reliable deliverability
// because each hop by acking the message, is saying as long as it exists it will deliver the messages
// the idle timeout should just be large




// add listener for StreamOpen
//    on stream open, add p2p transport to destination id

// initBuffer().SetNextHop()




// add forward listener. 
//      forward sequence with some small buffer, write to buffer drops immediately if no room




// NewP2pTransport(api, destination)
// inside the transport, every 1s when active, update the weights
// transports stays active until closed






// when buffer opens, create the transport
// when buffer times out, close the transport


// a stream replaces a single source and desintation with a route
// over arbitrarily many hops
// this manager sets up additional transports for each stream,
// which include a p2p transport


type StreamManager struct {
	ctx context.DialContext
	
	client *Client
	
	streamBuffer *StreamBuffer
}

func NewStreamManager(ctx, client, streamBufferSettings *StreamBufferSettings) *StreamManager {
	self.ctx = ctx
	self.client = client

	self.streamBuffer = NewStreamBuffer(ctx, client, streamBufferSettings)
}


// ReceiveFunction
func (self *ContractManager) Receive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
	switch sourceId {
	case ControlId:
		for _, frame := range frames {
			// ignore error
			self.handleControlMessage(frame)
		}
	}
}

func handleControlMessage(frame *protocol.Frame) error {
	if message, err := FromFrame(frame); err == nil {
		switch v := message.(type) {
		case *protocol.StreamOpen:
			var sourceId *Id
			if v.HasSourceId() {
				sourceId_, err := IdFromBytes(v.SourceId)
				if err != nil {
					return
				}
				sourceId = &sourceId_
			}

			var destinationId *Id
			if v.HasDestinationId() {
				destinationId_, err := IdFromBytes(v.DestinationId)
				if err != nil {
					return
				}
				destinationId = &destinationId_
			}
			
			streamId, err := IdFromBytes(v.StreamId)
			if err != nil {
				return
			}

			self.streamBuffer.OpenStream(sourceId, destinationId, streamId)

		case *protocol.StreamClose:
			streamId, err := IdFromBytes(v.StreamId)
			if err != nil {
				return
			}

			self.streamBuffer.CloseStream(streamId)
		}
	}
}


type streamSequenceId struct {
	SourceId Id
	DestinationId Id
	StreamId Id
}

func newStreamSequenceId(sourceId *Id, destinationId *Id, streamId Id) streamSequenceId {
	streamSequenceId := streamSequenceId{
		StreamId: streamId,
	}
	if sourceId != nil {
		streamSequenceId.SourceId = *sourceId
	}
	if destinationId != nil {
		streamSequenceId.DestinationId = *destinationId
	}
	return streamSequenceId
}


type StreamBuffer struct {
	ctx context.Context

	client *Client

	streamBufferSettings *StreamBufferSettings

	mutex sync.Mutex
	streamSequences map[streamSequenceId]*StreamSequence
	streamSequencesByStreamId map[Id]*StreamSequence
}

func NewStreamBuffer(ctx context.Context, client *Client, streamBufferSettings *StreamBufferSettings) *StreamBuffer {
	return &StreamBuffer{
		ctx: ctx,
		client: client,
		streamBufferSettings: streamBufferSettings,
		streamSequences: map[streamSequenceId]*StreamSequence{},
		streamSequencesByStreamId: map[Id]*StreamSequence{},
	}
}

func OpenStream(sourceId *Id, destinationId *Id, streamId Id) (bool, error) {
	streamSequenceId := newStreamSequenceId(sourceId, destinationId, streamId)

	initStreamSequence := func(skip *StreamSequence) *StreamSequence {
		mutex.Lock()
		defer mutex.Unlock()

		streamSequence, ok := streamSequences[streamSequenceId]
		if ok {
			if skip == nil || skip != streamSequence {
				return streamSequence
			} else {
				streamSequence.Cancel()
				delete(self.streamSequences, streamSequenceId)
			}
		}

		if streamSequenceByStreamId, ok := self.streamSequencesByStreamId[streamId]; ok {
			streamSequenceByStreamId.Cancel()
			delete(self.streamSequencesByStreamId, streamId)
		}

		streamSequence := NewStreamSequence()

		self.streamSequences[streamSequenceId] = streamSequence
		self.streamSequencesByStreamId[streamId] = streamSequence
		go func() {
			HandleError(streamSequence.Run)

			self.mutex.Lock()
			defer self.mutex.Unlock()
			streamSequence.Close()
			// clean up
			if streamSequence == self.streamSequences[streamSequenceId] {
				delete(self.streamSequences, streamSequenceId)
			}
			if streamSequence == self.streamSequencesByStreamId[streamId] {
				delete(self.streamSequencesByStreamId, streamId)
			}
		}()
		return sendSequence
	}

	var streamSequence *StreamSequence
	var success bool
	var err error
	for i := 0; i < 2; i += 1 {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		default:
		}
		streamSequence = initStreamSequence(streamSequence)
		if success, err = streamSequence.Open(); err == nil {
			return success, nil
		}
		// sequence closed
	}
	return success, err
	
	
}

func CloseStream(streamId Id) {
	mutex.Lock()
	defer mutex.Unlock()

	if streamSequenceByStreamId, ok := self.streamSequencesByStreamId[streamId]; ok {
		streamSequenceByStreamId.Cancel()
	}
}






type StreamSequence struct {
	ctx context.Context
	cancel context.CancelFunc

	client *Client

	sourceId *Id
	destinationId *Id
	streamId Id

	idleCondition *IdleCondition
}


func NewSendSequence(
		ctx context.Context,
		client *Client,
		sourceId *Id
		destinationId *Id
		streamId Id,
		streamBufferSettings *StreamBufferSettings) *StreamSequence {
	cancelCtx, cancel := context.WithCancel(ctx)

	return &StreamSequence{
		ctx: cancelCtx,
		cancel: cancel,
		client: client,
		sourceId: sourceId,
		destinationId: destinationId,
		streamId: streamId,
		idleCondition: NewIdleCondition(),
	}
}


func (self *StreamSequence) Open() (bool, error) {
	select {
	case <- self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	if !self.idleCondition.UpdateOpen() {
		return false, errors.New("Done.")
	}
	defer self.idleCondition.UpdateClose()

	return true, nil
}


func (self *StreamSequence) Run() {	
	defer self.cancel()

	if self.sourceId == nil || self.destinationId == nil {
		clientRouteManager := client.RouteManager()

		sendReady := make(chan struct{})
		receiveReady := make(chan struct{})
		var p2pTransport *P2pTransport
		if self.sourceId != nil {
			p2pTransport := NewP2pTransport(
				self.ctx,
				self.client,
				clientRouteManager,
				clientRouteManager,
				self.sourceId,
				self.streamId,
				PeerTypeSource,
				sendReady,
				receiveReady,
			)
		} else {
			p2pTransport := NewP2pTransport(
				self.ctx,
				self.client,
				clientRouteManager,
				clientRouteManager,
				self.destinationId,
				self.streamId,
				PeerTypeDestination,
				sendReady,
				receiveReady,
			)
		}
		// this will propagate to the other side of the stream
		// p2pTransport.SetReceiveReady(true)
		close(receiveReady)
	} else {
		p2pToDestinationRouteManager := NewRouteManager(ctx, fmt.Sprintf("->s(%s)", self.streamId))
		p2pToSourceRouteManager := NewRouteManager(ctx, fmt.Sprintf("<-s(%s)", self.streamId))

		toDestinationReady := make(chan struct{})
		toSourceReady := make(chan struct{})
		toDestinationTransport := NewP2pTransport(
			self.ctx,
			self.client,
			p2pToDestinationRouteManager,
			p2pToSourceRouteManager,
			self.destinationId,
			self.streamId,
			PeerTypeDestination,
			toDestinationReady,
			toSourceReady,
		)
		toSourceTransport := NewP2pTransport(
			self.ctx,
			self.client,
			registerRouteManager,
			p2pToSourceRouteManager,
			p2pToDestinationRouteManager,
			self.sourceId,
			self.streamId,
			PeerTypeSource,
			toSourceReady,
			toDestinationReady,
		)

		forward := func(routeManager *RouteManager) {
			defer self.cancel()

			mrr := routeManager.OpenMultiRouteReader(TransferPath{
				StreamId: self.streamId,
			})
			defer routeManager.CloseMultiRouteReader(mrr)
			mrw := routeManager.OpenMultiRouteWriter(TransferPath{
				StreamId: self.streamId,
			})
			defer routeManager.CloseMultiRouteWriter(mrw)

			for {
				select {
				case <- self.ctx.Done():
					return
				}

				checkpointId := self.idleCondition.Checkpoint()
				transferFrameBytes, err := mrr.Read(self.ctx, TIMEOUT)
				if transferFrameBytes == nil && err == nil {
					// idle timeout
					if self.idleCondition.Close(checkpointId) {
						// close the sequence
					    return
					}
					// else the sequence was opened again
					continue
				}
				success, err := mrw.Write(self.ctx, transferFrameBytes, WRITE_TIMEOUT)
				if !success {
					// drop it
				}
			}
		}()

		go forward(p2pToDestinationRouteManager)
		go forward(p2pToSourceRouteManager)
	}

	select {
	case <- self.ctx.Done():
		return
	}

	// FIXME Transfer will need to switch multi route writers when the contract stream id changes
	// FIXME close stream when stream switches, and when sender or receiver closes
	// FIXME close all pending contracts when sender or receiver closes
}

func (self *StreamSequence) Close() {
	cancel()
}





package connect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"

	"bringyour.com/protocol"
)

func DefaultStreamManagerSettings() *StreamManagerSettings {
	return &StreamManagerSettings{
		StreamBufferSettings: DefaultStreamBufferSettings(),
		WebRtcSettings:       DefaultWebRtcSettings(),
	}
}

func DefaultStreamBufferSettings() *StreamBufferSettings {
	return &StreamBufferSettings{
		ReadTimeout:          time.Duration(-1),
		WriteTimeout:         time.Duration(-1),
		P2pTransportSettings: DefaultP2pTransportSettings(),
	}
}

type StreamManagerSettings struct {
	StreamBufferSettings *StreamBufferSettings

	WebRtcSettings *WebRtcSettings
}

type StreamManager struct {
	ctx context.Context

	client *Client

	webRtcManager *WebRtcManager

	streamBuffer *StreamBuffer

	streamManagerSettings *StreamManagerSettings
}

func NewStreamManager(ctx context.Context, client *Client, streamManagerSettings *StreamManagerSettings) *StreamManager {
	streamManager := &StreamManager{
		ctx:                   ctx,
		client:                client,
		streamManagerSettings: streamManagerSettings,
	}

	webRtcManager := NewWebRtcManager(ctx, streamManagerSettings.WebRtcSettings)

	streamManager.initBuffers(webRtcManager)

	return streamManager
}

func (self *StreamManager) initBuffers(webRtcManager *WebRtcManager) {
	self.webRtcManager = webRtcManager
	self.streamBuffer = NewStreamBuffer(self.ctx, self, self.streamManagerSettings.StreamBufferSettings)
}

func (self *StreamManager) Client() *Client {
	return self.client
}

func (self *StreamManager) WebRtcManager() *WebRtcManager {
	return self.webRtcManager
}

// ReceiveFunction
func (self *StreamManager) Receive(source TransferPath, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
	if source.IsControlSource() {
		for _, frame := range frames {
			// ignore error
			self.handleControlFrame(frame)
		}
	}
}

func (self *StreamManager) handleControlFrame(frame *protocol.Frame) error {
	if message, err := FromFrame(frame); err == nil {
		switch v := message.(type) {
		case *protocol.StreamOpen:
			var sourceId *Id
			if v.SourceId != nil {
				sourceId_, err := IdFromBytes(v.SourceId)
				if err != nil {
					return err
				}
				sourceId = &sourceId_
			}

			var destinationId *Id
			if v.DestinationId != nil {
				destinationId_, err := IdFromBytes(v.DestinationId)
				if err != nil {
					return err
				}
				destinationId = &destinationId_
			}

			streamId, err := IdFromBytes(v.StreamId)
			if err != nil {
				return err
			}

			self.streamBuffer.OpenStream(sourceId, destinationId, streamId)

		case *protocol.StreamClose:
			streamId, err := IdFromBytes(v.StreamId)
			if err != nil {
				return err
			}

			self.streamBuffer.CloseStream(streamId)
		}
	}
	return nil
}

func (self *StreamManager) IsStreamOpen(streamId Id) bool {
	return self.streamBuffer.IsStreamOpen(streamId)
}

type StreamBufferSettings struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	P2pTransportSettings *P2pTransportSettings
}

type streamSequenceId struct {
	SourceId       Id
	HasSource      bool
	DestinationId  Id
	HasDestination bool
	StreamId       Id
}

func newStreamSequenceId(sourceId *Id, destinationId *Id, streamId Id) streamSequenceId {
	streamSequenceId := streamSequenceId{
		StreamId: streamId,
	}
	if sourceId != nil {
		streamSequenceId.SourceId = *sourceId
		streamSequenceId.HasSource = true
	}
	if destinationId != nil {
		streamSequenceId.DestinationId = *destinationId
		streamSequenceId.HasDestination = true
	}
	return streamSequenceId
}

type StreamBuffer struct {
	ctx context.Context

	streamManager *StreamManager

	streamBufferSettings *StreamBufferSettings

	mutex                     sync.Mutex
	streamSequences           map[streamSequenceId]*StreamSequence
	streamSequencesByStreamId map[Id]*StreamSequence
}

func NewStreamBuffer(ctx context.Context, streamManager *StreamManager, streamBufferSettings *StreamBufferSettings) *StreamBuffer {
	return &StreamBuffer{
		ctx:                       ctx,
		streamManager:             streamManager,
		streamBufferSettings:      streamBufferSettings,
		streamSequences:           map[streamSequenceId]*StreamSequence{},
		streamSequencesByStreamId: map[Id]*StreamSequence{},
	}
}

func (self *StreamBuffer) OpenStream(sourceId *Id, destinationId *Id, streamId Id) (bool, error) {
	streamSequenceId := newStreamSequenceId(sourceId, destinationId, streamId)

	initStreamSequence := func(skip *StreamSequence) *StreamSequence {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		streamSequence, ok := self.streamSequences[streamSequenceId]
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

		streamSequence = NewStreamSequence(self.ctx, self.streamManager, sourceId, destinationId, streamId, self.streamBufferSettings)

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
		return streamSequence
	}

	var streamSequence *StreamSequence
	var success bool
	var err error
	for i := 0; i < 2; i += 1 {
		select {
		case <-self.ctx.Done():
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

func (self *StreamBuffer) CloseStream(streamId Id) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if streamSequence, ok := self.streamSequencesByStreamId[streamId]; ok {
		streamSequence.Cancel()
	}
}

func (self *StreamBuffer) IsStreamOpen(streamId Id) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	_, ok := self.streamSequencesByStreamId[streamId]
	return ok
}

type StreamSequence struct {
	ctx    context.Context
	cancel context.CancelFunc

	streamManager *StreamManager

	streamBufferSettings *StreamBufferSettings

	sourceId      *Id
	destinationId *Id
	streamId      Id

	idleCondition *IdleCondition
}

func NewStreamSequence(
	ctx context.Context,
	streamManager *StreamManager,
	sourceId *Id,
	destinationId *Id,
	streamId Id,
	streamBufferSettings *StreamBufferSettings) *StreamSequence {
	cancelCtx, cancel := context.WithCancel(ctx)

	return &StreamSequence{
		ctx:                  cancelCtx,
		cancel:               cancel,
		streamManager:        streamManager,
		streamBufferSettings: streamBufferSettings,
		sourceId:             sourceId,
		destinationId:        destinationId,
		streamId:             streamId,
		idleCondition:        NewIdleCondition(),
	}
}

func (self *StreamSequence) Open() (bool, error) {
	select {
	case <-self.ctx.Done():
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
		clientRouteManager := self.streamManager.Client().RouteManager()

		sendReady := make(chan struct{})
		receiveReady := make(chan struct{})
		if self.sourceId != nil {
			NewP2pTransport(
				self.ctx,
				self.streamManager.Client(),
				self.streamManager.WebRtcManager(),
				clientRouteManager,
				clientRouteManager,
				*self.sourceId,
				self.streamId,
				PeerTypeSource,
				sendReady,
				receiveReady,
				self.streamBufferSettings.P2pTransportSettings,
			)
		} else if self.destinationId != nil {
			NewP2pTransport(
				self.ctx,
				self.streamManager.Client(),
				self.streamManager.WebRtcManager(),
				clientRouteManager,
				clientRouteManager,
				*self.destinationId,
				self.streamId,
				PeerTypeDestination,
				sendReady,
				receiveReady,
				self.streamBufferSettings.P2pTransportSettings,
			)
		} else {
			// the stream must have one of source or destination
			glog.V(1).Infof("[sm] s(%s) missing source or destination.\n", self.streamId)
			return
		}

		// this will propagate to the other side of the stream
		// p2pTransport.SetReceiveReady(true)
		close(receiveReady)
	} else {
		p2pToDestinationRouteManager := NewRouteManager(self.ctx, fmt.Sprintf("->s(%s)", self.streamId))
		p2pToSourceRouteManager := NewRouteManager(self.ctx, fmt.Sprintf("<-s(%s)", self.streamId))

		toDestinationReady := make(chan struct{})
		toSourceReady := make(chan struct{})
		// to destination
		NewP2pTransport(
			self.ctx,
			self.streamManager.Client(),
			self.streamManager.WebRtcManager(),
			p2pToDestinationRouteManager,
			p2pToSourceRouteManager,
			*self.destinationId,
			self.streamId,
			PeerTypeDestination,
			toDestinationReady,
			toSourceReady,
			self.streamBufferSettings.P2pTransportSettings,
		)
		// to source
		NewP2pTransport(
			self.ctx,
			self.streamManager.Client(),
			self.streamManager.WebRtcManager(),
			p2pToSourceRouteManager,
			p2pToDestinationRouteManager,
			*self.sourceId,
			self.streamId,
			PeerTypeSource,
			toSourceReady,
			toDestinationReady,
			self.streamBufferSettings.P2pTransportSettings,
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
				case <-self.ctx.Done():
					return
				}

				checkpointId := self.idleCondition.Checkpoint()
				transferFrameBytes, err := mrr.Read(self.ctx, self.streamBufferSettings.ReadTimeout)
				if transferFrameBytes == nil && err == nil {
					// idle timeout
					if self.idleCondition.Close(checkpointId) {
						// close the sequence
						return
					}
					// else the sequence was opened again
					continue
				}
				success, err := mrw.WriteDetailed(self.ctx, transferFrameBytes, self.streamBufferSettings.WriteTimeout)
				if err != nil {
					return
				}
				if !success {
					// drop it
				}
			}
		}

		go forward(p2pToDestinationRouteManager)
		go forward(p2pToSourceRouteManager)
	}

	select {
	case <-self.ctx.Done():
		return
	}
}

func (self *StreamSequence) Cancel() {
	self.cancel()
}

func (self *StreamSequence) Close() {
	self.cancel()
}

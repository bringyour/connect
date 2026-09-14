//go:build !js

// This file attaches the native authenticated datagram carrier to Pion's
// already-negotiated ICE, DTLS, and SRTP stack. A custom RTP codec is only a
// capability marker; its payload is Connect's independently fragmented,
// end-to-end-encrypted transfer message.
package connect

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// webRtcFastPathTrack records whether the remote description accepted the
// custom codec. TrackLocalStaticRTP otherwise treats an unbound write as a
// successful write to zero bindings, which would silently drop traffic.
type webRtcFastPathTrack struct {
	track              *webrtc.TrackLocalStaticRTP
	capability         webrtc.RTPCodecCapability
	bound              atomic.Bool
	rejectedBindingIds sync.Map
}

// Bind delegates codec matching and publishes readiness only after success.
func (self *webRtcFastPathTrack) Bind(
	trackContext webrtc.TrackLocalContext,
) (webrtc.RTPCodecParameters, error) {
	parameters, err := self.track.Bind(trackContext)
	if err == nil {
		self.rejectedBindingIds.Delete(trackContext.ID())
		self.bound.Store(true)
		return parameters, nil
	}
	if errors.Is(err, webrtc.ErrUnsupportedCodec) {
		// Pion normally makes a rejected optional media section fail the entire
		// SetRemoteDescription call. Record the rejected binding and satisfy the
		// sender lifecycle without installing a writer; the data channel can then
		// complete and Auto mode observes bound=false and falls back.
		self.rejectedBindingIds.Store(trackContext.ID(), struct{}{})
		return webrtc.RTPCodecParameters{
			RTPCodecCapability: self.capability,
			PayloadType:        p2pFastPathRtpPayloadType,
		}, nil
	}
	return parameters, err
}

// Unbind retires readiness before releasing the Pion binding.
func (self *webRtcFastPathTrack) Unbind(trackContext webrtc.TrackLocalContext) error {
	self.bound.Store(false)
	if _, rejected := self.rejectedBindingIds.LoadAndDelete(trackContext.ID()); rejected {
		return nil
	}
	return self.track.Unbind(trackContext)
}

// ID returns the stable per-peer track id.
func (self *webRtcFastPathTrack) ID() string {
	return self.track.ID()
}

// RID returns the RTP stream id, which is unused by this carrier.
func (self *webRtcFastPathTrack) RID() string {
	return self.track.RID()
}

// StreamID returns the stable per-peer media stream id.
func (self *webRtcFastPathTrack) StreamID() string {
	return self.track.StreamID()
}

// Kind identifies the custom codec as a video-shaped RTP carrier.
func (self *webRtcFastPathTrack) Kind() webrtc.RTPCodecType {
	return self.track.Kind()
}

// webRtcFastPath owns one directional RTP sender and one independently
// negotiated receiver. The send mutex assigns message and RTP counters in
// order; the receive callback starts exactly one bounded reassembly worker.
type webRtcFastPath struct {
	ctx                       context.Context
	log                       Logger
	maximumMessageByteCount   int
	dataPlaneStats            *P2pDataPlaneStats
	track                     *webRtcFastPathTrack
	messages                  chan p2pFastPathReceivedMessage
	receiveOnce               sync.Once
	receiveDone               chan struct{}
	warmupOnce                sync.Once
	readyOnce                 sync.Once
	ready                     chan struct{}
	receiveReady              atomic.Bool
	sendMutex                 sync.Mutex
	nextMessageId             uint32
	nextSequenceNumber        uint16
	warmupVersion             byte
	afterWarmupReceiveForTest func(byte)

	// Tests retain an exact reassembly-buffer witness before queue handoff.
	// Nil is a production no-op.
	afterReceiveMessageAllocatedForTest func([]byte)
}

// p2pFastPathPacketReader is the cancellation-aware packet boundary owned by
// one native SRTP receive worker.
type p2pFastPathPacketReader interface {
	Read(packet []byte) (int, error)
	SetReadDeadline(deadline time.Time) error
}

// webRtcFastPathTrackReader adapts Pion's attribute-bearing track read to the
// packet-only boundary used by the fast-path worker.
type webRtcFastPathTrackReader struct {
	track *webrtc.TrackRemote
}

// Read returns one decrypted RTP packet and discards interceptor attributes.
func (self *webRtcFastPathTrackReader) Read(packet []byte) (int, error) {
	packetByteCount, _, err := self.track.Read(packet)
	return packetByteCount, err
}

// SetReadDeadline interrupts an outstanding Pion track read at teardown.
func (self *webRtcFastPathTrackReader) SetReadDeadline(deadline time.Time) error {
	return self.track.SetReadDeadline(deadline)
}

// newWebRtcMediaEngine creates the immutable codec registry used by one Pion
// API. Legacy-only registries remain media-free.
func newWebRtcMediaEngine(settings *WebRtcSettings) (*webrtc.MediaEngine, error) {
	mediaEngine := &webrtc.MediaEngine{}
	if !settings.EnableDatagramFastPath {
		return mediaEngine, nil
	}
	err := mediaEngine.RegisterCodec(
		webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  p2pFastPathMimeType,
				ClockRate: p2pFastPathRtpClockRate,
			},
			PayloadType: p2pFastPathRtpPayloadType,
		},
		webrtc.RTPCodecTypeVideo,
	)
	return mediaEngine, err
}

// p2pFastPathWarmupVersionForSettings returns the production wire version
// unless a test is constructing the other side of a rolling-upgrade boundary.
func p2pFastPathWarmupVersionForSettings(settings *WebRtcSettings) byte {
	if settings.datagramFastPathWarmupVersionForTest != 0 {
		return settings.datagramFastPathWarmupVersionForTest
	}
	return p2pFastPathVersion
}

// configureFastPath adds a sendrecv media section before offer/answer
// creation. A peer that does not advertise the codec simply leaves the local
// track unbound, and Auto mode retains the reliable DataChannel fallback.
func (self *peerConn) configureFastPath() error {
	if !self.settings.EnableDatagramFastPath {
		return nil
	}
	capability := webrtc.RTPCodecCapability{
		MimeType:  p2pFastPathMimeType,
		ClockRate: p2pFastPathRtpClockRate,
	}
	track, err := webrtc.NewTrackLocalStaticRTP(
		capability,
		"urnetwork-fast-path",
		"urnetwork-fast-path",
	)
	if err != nil {
		return err
	}
	fastPath := &webRtcFastPath{
		ctx:                     self.ctx,
		log:                     self.log,
		maximumMessageByteCount: int(self.settings.MaxMessageSize),
		dataPlaneStats:          self.settings.DataPlaneStats,
		track: &webRtcFastPathTrack{
			track:      track,
			capability: capability,
		},
		messages: make(
			chan p2pFastPathReceivedMessage,
			max(1, self.settings.DatagramFastPathReceiveBufferSize),
		),
		receiveDone: make(chan struct{}),
		ready:       make(chan struct{}),
		warmupVersion: p2pFastPathWarmupVersionForSettings(
			self.settings,
		),
		afterWarmupReceiveForTest: self.settings.
			afterFastPathWarmupReceiveForTest,
	}
	self.fastPath.Store(fastPath)
	if self.settings.afterFastPathPublishForTest != nil {
		self.settings.afterFastPathPublishForTest()
	}
	var sender *webrtc.RTPSender
	err = self.withPionMutation(func() error {
		if self.fastPath.Load() != fastPath {
			return context.Canceled
		}
		self.pc.OnTrack(func(
			remoteTrack *webrtc.TrackRemote,
			receiver *webrtc.RTPReceiver,
		) {
			self.runPionCallback("fast path track callback", func() {
				if self.settings.beforeFastPathOnTrackBodyForTest != nil {
					self.settings.beforeFastPathOnTrackBodyForTest()
				}
				_ = receiver
				if !strings.EqualFold(remoteTrack.Codec().MimeType, p2pFastPathMimeType) {
					return
				}
				fastPath.startReceive(&webRtcFastPathTrackReader{track: remoteTrack})
			}, self.cancel)
		})
		sender, err = self.pc.AddTrack(fastPath.track)
		return err
	})
	if err != nil {
		if self.fastPath.CompareAndSwap(fastPath, nil) {
			fastPath.closeAndWait()
		}
		return err
	}
	if self.fastPath.Load() != fastPath {
		if err := context.Cause(self.ctx); err != nil {
			return err
		}
		return context.Canceled
	}
	self.startWorker("fast path RTCP drain", func() {
		buffer := make([]byte, 1500)
		for {
			if _, _, readErr := sender.Read(buffer); readErr != nil {
				return
			}
		}
	})
	return nil
}

// Retires the published native carrier exactly once across setup and teardown.
func (self *peerConn) closeFastPath() {
	if fastPath := self.fastPath.Swap(nil); fastPath != nil {
		fastPath.closeAndWait()
	}
}

// startReceive starts exactly one receive worker unless teardown already won
// the generation's lifecycle boundary.
func (self *webRtcFastPath) startReceive(reader p2pFastPathPacketReader) {
	self.receiveOnce.Do(func() {
		go HandleError(func() {
			defer close(self.receiveDone)
			defer close(self.messages)
			self.readTrack(reader)
		})
	})
}

// closeAndWait prevents late receive admission, joins the native reader, and
// returns complete messages left in its bounded handoff queue.
func (self *webRtcFastPath) closeAndWait() {
	self.receiveOnce.Do(func() {
		close(self.messages)
		close(self.receiveDone)
	})
	<-self.receiveDone
	for received := range self.messages {
		MessagePoolReturn(received.message)
	}
}

// FastPathReady reports a mutually negotiated and currently bound custom
// codec. It does not infer readiness from SDP presence alone.
func (self *peerConn) FastPathReady() bool {
	fastPath := self.fastPath.Load()
	return fastPath != nil &&
		fastPath.track.bound.Load() &&
		fastPath.receiveReady.Load()
}

// WaitFastPathReady waits only for a forced carrier selection. Auto mode
// remains free to use the reliable compatibility lane immediately, while a
// deterministic FastOnly route gives the mutually supported RTP workers time
// to exchange their setup markers before judging negotiation unsuccessful.
func (self *peerConn) WaitFastPathReady(
	ctx context.Context,
	timeout time.Duration,
) bool {
	if self.FastPathReady() {
		return true
	}
	fastPath := self.fastPath.Load()
	if fastPath == nil || timeout <= 0 {
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-self.ctx.Done():
		return false
	case <-fastPath.ready:
		return self.FastPathReady()
	case <-timer.C:
		return false
	}
}

// FastPathMessages returns the ownership-transferring receive queue.
func (self *peerConn) FastPathMessages() <-chan p2pFastPathReceivedMessage {
	fastPath := self.fastPath.Load()
	if fastPath == nil {
		return nil
	}
	return fastPath.messages
}

// WriteFastPathMessage fragments one complete transfer message into
// independently authenticated RTP/SRTP datagrams. A caller may retry the same
// transfer over the legacy channel after an error; Transfer message identity
// makes that duplicate harmless.
func (self *peerConn) WriteFastPathMessage(message []byte) (int, error) {
	fastPath := self.fastPath.Load()
	if fastPath == nil || !fastPath.track.bound.Load() {
		return 0, errP2pFastPathNotReady
	}
	return fastPath.writeMessage(message)
}

// writeMessage assigns ordered fragment counters and writes every fragment
// synchronously before the caller releases the source message.
func (self *webRtcFastPath) writeMessage(message []byte) (int, error) {
	if len(message) == 0 || self.maximumMessageByteCount < len(message) {
		return 0, errP2pFastPathMessageTooLarge
	}
	fragmentCount := p2pFastPathFragmentCount(len(message))
	if p2pFastPathMaximumFragmentCount < fragmentCount {
		return 0, errP2pFastPathMessageTooLarge
	}

	self.sendMutex.Lock()
	defer self.sendMutex.Unlock()
	self.nextMessageId += 1
	if self.nextMessageId == 0 {
		self.nextMessageId = 1
	}
	messageId := self.nextMessageId
	var packetBuffer [p2pFastPathFragmentHeaderByteCount + p2pFastPathFragmentPayloadByteCount]byte
	for fragmentIndex := range fragmentCount {
		offset := fragmentIndex * p2pFastPathFragmentPayloadByteCount
		fragmentByteCount := min(
			p2pFastPathFragmentPayloadByteCount,
			len(message)-offset,
		)
		packet := packetBuffer[:p2pFastPathFragmentHeaderByteCount+fragmentByteCount]
		err := writeP2pFastPathFragmentHeader(
			packet,
			p2pFastPathFragmentHeader{
				messageId:     messageId,
				messageLength: len(message),
				fragmentIndex: fragmentIndex,
				fragmentCount: fragmentCount,
			},
		)
		if err != nil {
			return fragmentIndex, err
		}
		copy(
			packet[p2pFastPathFragmentHeaderByteCount:],
			message[offset:offset+fragmentByteCount],
		)
		self.nextSequenceNumber += 1
		rtpPacket := rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         fragmentIndex+1 == fragmentCount,
				SequenceNumber: self.nextSequenceNumber,
				Timestamp:      messageId,
			},
			Payload: packet,
		}
		if err := self.track.track.WriteRTP(&rtpPacket); err != nil {
			return fragmentIndex, err
		}
	}
	self.dataPlaneStats.observeFastSendFragments(fragmentCount)
	return fragmentCount, nil
}

// startFastPathWarmup starts the generation-local native readiness exchange.
func (self *peerConn) startFastPathWarmup() {
	if fastPath := self.fastPath.Load(); fastPath != nil {
		fastPath.warmupOnce.Do(func() {
			self.startWorker("fast path warmup", fastPath.runWarmup)
		})
	}
}

// runWarmup sends a tiny carrier marker until a remote marker proves that
// both RTP receive workers are active. Pion fires OnTrack from the first RTP
// packet; repeating only during this setup window prevents that trigger packet
// from becoming a cold-path application loss.
func (self *webRtcFastPath) runWarmup() {
	ticker := time.NewTicker(p2pFastPathWarmupInterval)
	defer ticker.Stop()
	timer := time.NewTimer(p2pFastPathWarmupTimeout)
	defer timer.Stop()
	for {
		if self.track.bound.Load() {
			if err := self.writeWarmup(); err != nil {
				if self.log.V(1).Enabled() {
					self.log.Infof("[p2p-fast]warmup err = %s\n", err)
				}
				return
			}
			if self.receiveReady.Load() {
				return
			}
		}
		select {
		case <-self.ctx.Done():
			return
		case <-ticker.C:
		case <-timer.C:
			return
		}
	}
}

// writeWarmup emits one marker under the same sequence-number lock as data.
func (self *webRtcFastPath) writeWarmup() error {
	self.sendMutex.Lock()
	defer self.sendMutex.Unlock()
	self.nextSequenceNumber += 1
	return self.track.track.WriteRTP(&rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			SequenceNumber: self.nextSequenceNumber,
		},
		Payload: []byte{'U', 'R', 'W', self.warmupVersion},
	})
}

// readTrack drains decrypted SRTP packets, validates the RTP header, and
// passes only complete bounded messages to the P2P route worker.
func (self *webRtcFastPath) readTrack(reader p2pFastPathPacketReader) {
	reassembler := newP2pFastPathReassembler(self.maximumMessageByteCount)
	reassembler.afterMessageAllocatedForTest = self.afterReceiveMessageAllocatedForTest
	reassembler.dataPlaneStats = self.dataPlaneStats
	defer reassembler.close()
	readDeadlineDone := make(chan struct{})
	var readDeadlineDoneOnce sync.Once
	stopReadDeadline := context.AfterFunc(self.ctx, func() {
		defer readDeadlineDoneOnce.Do(func() {
			close(readDeadlineDone)
		})
		_ = reader.SetReadDeadline(time.Now())
	})
	defer func() {
		if stopReadDeadline() {
			readDeadlineDoneOnce.Do(func() {
				close(readDeadlineDone)
			})
		}
		<-readDeadlineDone
	}()
	for {
		packetBuffer := MessagePoolGet(2048)
		packetByteCount, err := reader.Read(packetBuffer)
		if err != nil {
			MessagePoolReturn(packetBuffer)
			if !errors.Is(err, io.EOF) && self.ctx.Err() == nil && self.log.V(1).Enabled() {
				self.log.Infof("[p2p-fast]receive err = %s\n", err)
			}
			return
		}
		var header rtp.Header
		headerByteCount, headerErr := header.Unmarshal(packetBuffer[:packetByteCount])
		if headerErr != nil || header.Version != 2 || packetByteCount <= headerByteCount {
			if self.dataPlaneStats != nil {
				self.dataPlaneStats.fastDropCount.Add(1)
			}
			MessagePoolReturn(packetBuffer)
			continue
		}
		payload := packetBuffer[headerByteCount:packetByteCount]
		if len(payload) == 4 &&
			payload[0] == 'U' &&
			payload[1] == 'R' &&
			payload[2] == 'W' {
			if self.afterWarmupReceiveForTest != nil {
				self.afterWarmupReceiveForTest(payload[3])
			}
			if payload[3] == self.warmupVersion {
				self.receiveReady.Store(true)
				self.readyOnce.Do(func() {
					close(self.ready)
				})
			}
			MessagePoolReturn(packetBuffer)
			continue
		}
		message, acceptErr := reassembler.accept(
			payload,
			time.Now(),
		)
		MessagePoolReturn(packetBuffer)
		if acceptErr != nil {
			if self.dataPlaneStats != nil {
				self.dataPlaneStats.fastDropCount.Add(1)
			}
			continue
		}
		if message == nil {
			continue
		}
		fragmentCount := p2pFastPathFragmentCount(len(message))
		select {
		case <-self.ctx.Done():
			MessagePoolReturn(message)
			return
		case self.messages <- p2pFastPathReceivedMessage{
			message:       message,
			fragmentCount: fragmentCount,
		}:
		default:
			MessagePoolReturn(message)
			if self.dataPlaneStats != nil {
				self.dataPlaneStats.fastDropCount.Add(1)
			}
		}
	}
}

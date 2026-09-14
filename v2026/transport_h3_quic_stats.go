package connect

// This file exposes an opt-in, lock-free measurement boundary between
// Conn.SendDatagram queue admission and the application's ReceiveDatagram
// dequeue. Default client settings supply a counter set so PTO and handshake
// blackholes are observable in production; callers can still set it to nil on
// a custom settings value to disable tracing. The server's analogous setting
// remains opt-in.

import (
	"context"
	"sync"
	"sync/atomic"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// H3QuicPacketStatsSnapshot is a lifetime view of QUIC packet events. DATAGRAM
// frame counts are actual frames selected for a QUIC packet, unlike
// H3DatagramStats.SentMessageCount, which records successful admission to
// quic-go's bounded send queue. Comparing received frame counts with
// H3DatagramStats.ReceivedFragmentCount exposes drops in quic-go's bounded
// application receive queue.
type H3QuicPacketStatsSnapshot struct {
	ConnectionCount       uint64
	ClosedConnectionCount uint64

	SentPacketCount         uint64
	SentPacketByteCount     uint64
	SentDatagramPacketCount uint64
	SentDatagramFrameCount  uint64
	SentDatagramByteCount   uint64

	ReceivedPacketCount         uint64
	ReceivedPacketByteCount     uint64
	ReceivedDatagramPacketCount uint64
	ReceivedDatagramFrameCount  uint64
	ReceivedDatagramByteCount   uint64

	DroppedPacketCount                             uint64
	DroppedPacketByteCount                         uint64
	DroppedDosPreventionPacketCount                uint64
	DroppedDuplicatePacketCount                    uint64
	DroppedOtherPacketCount                        uint64
	DroppedKeyUnavailablePacketCount               uint64
	DroppedUnknownConnectionIdPacketCount          uint64
	DroppedHeaderParseErrorPacketCount             uint64
	DroppedPayloadDecryptErrorPacketCount          uint64
	DroppedProtocolViolationPacketCount            uint64
	DroppedUnsupportedVersionPacketCount           uint64
	DroppedUnexpectedPacketCount                   uint64
	DroppedUnexpectedSourceConnectionIdPacketCount uint64
	DroppedUnexpectedVersionPacketCount            uint64
	DroppedPayloadDecryptBeforeKeyUpdateCount      uint64
	DroppedPayloadDecryptAfterKeyUpdateCount       uint64
	LocalKeyUpdateCount                            uint64
	RemoteKeyUpdateCount                           uint64
	KeyDiscardCount                                uint64
	LostPacketCount                                uint64
	ProbeTimeoutCount                              uint64
	HandshakeAttemptCount                          uint64
	HandshakeSuccessCount                          uint64
	HandshakeFailureCount                          uint64
	HandshakeSentWithoutResponseCount              uint64
	MtuUpdateCount                                 uint64
	CurrentMtu                                     int64
}

// H3QuicPacketFingerprintStats retains only bounded CRC32C fingerprints that
// quic-go already computes over complete encrypted UDP datagrams for qlog. It
// is an opt-in diagnostic for correlating a sender's packet with a receiver's
// drop without retaining packet payloads. CRC32C is not collision resistant;
// this evidence is suitable for controlled tests, not security decisions.
type H3QuicPacketFingerprintStats struct {
	stateLock sync.Mutex
	maxCount  int

	sent                    map[uint32]uint64
	received                map[uint32]uint64
	droppedPayloadDecrypt   map[uint32]uint64
	refusedFingerprintCount uint64
	unavailableCount        uint64
}

// H3QuicPacketFingerprintStatsSnapshot owns independent maps and can be read
// while its traced QUIC connections continue to run.
type H3QuicPacketFingerprintStatsSnapshot struct {
	Sent                    map[uint32]uint64
	Received                map[uint32]uint64
	DroppedPayloadDecrypt   map[uint32]uint64
	RefusedFingerprintCount uint64
	UnavailableCount        uint64
}

func NewH3QuicPacketFingerprintStats(maxCount int) *H3QuicPacketFingerprintStats {
	return &H3QuicPacketFingerprintStats{
		maxCount:              max(1, maxCount),
		sent:                  map[uint32]uint64{},
		received:              map[uint32]uint64{},
		droppedPayloadDecrypt: map[uint32]uint64{},
	}
}

func (self *H3QuicPacketFingerprintStats) record(
	fingerprints map[uint32]uint64,
	checksum qlog.DatagramPayloadChecksum,
) {
	if checksum == 0 {
		self.stateLock.Lock()
		self.unavailableCount += 1
		self.stateLock.Unlock()
		return
	}
	value := uint32(checksum)
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if _, ok := fingerprints[value]; !ok && self.maxCount <= len(fingerprints) {
		self.refusedFingerprintCount += 1
		return
	}
	fingerprints[value] += 1
}

func (self *H3QuicPacketFingerprintStats) recordSent(
	checksum qlog.DatagramPayloadChecksum,
) {
	if self != nil {
		self.record(self.sent, checksum)
	}
}

func (self *H3QuicPacketFingerprintStats) recordReceived(
	checksum qlog.DatagramPayloadChecksum,
) {
	if self != nil {
		self.record(self.received, checksum)
	}
}

func (self *H3QuicPacketFingerprintStats) recordDroppedPayloadDecrypt(
	checksum qlog.DatagramPayloadChecksum,
) {
	if self != nil {
		self.record(self.droppedPayloadDecrypt, checksum)
	}
}

func cloneH3QuicPacketFingerprints(values map[uint32]uint64) map[uint32]uint64 {
	result := make(map[uint32]uint64, len(values))
	for checksum, count := range values {
		result[checksum] = count
	}
	return result
}

func (self *H3QuicPacketFingerprintStats) Snapshot() H3QuicPacketFingerprintStatsSnapshot {
	if self == nil {
		return H3QuicPacketFingerprintStatsSnapshot{}
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return H3QuicPacketFingerprintStatsSnapshot{
		Sent:                    cloneH3QuicPacketFingerprints(self.sent),
		Received:                cloneH3QuicPacketFingerprints(self.received),
		DroppedPayloadDecrypt:   cloneH3QuicPacketFingerprints(self.droppedPayloadDecrypt),
		RefusedFingerprintCount: self.refusedFingerprintCount,
		UnavailableCount:        self.unavailableCount,
	}
}

// H3QuicPacketStats aggregates reconnect generations without retaining qlog
// events or packet payloads. Supplying it enables quic-go tracing, so callers
// should use it for diagnostics and benchmarks rather than routine telemetry.
type H3QuicPacketStats struct {
	// PacketFingerprints is nil on the normal counter-only path. A caller that
	// supplies it accepts bounded map/mutex overhead for packet correlation.
	PacketFingerprints *H3QuicPacketFingerprintStats

	connectionCount       atomic.Uint64
	closedConnectionCount atomic.Uint64

	sentPacketCount         atomic.Uint64
	sentPacketByteCount     atomic.Uint64
	sentDatagramPacketCount atomic.Uint64
	sentDatagramFrameCount  atomic.Uint64
	sentDatagramByteCount   atomic.Uint64

	receivedPacketCount         atomic.Uint64
	receivedPacketByteCount     atomic.Uint64
	receivedDatagramPacketCount atomic.Uint64
	receivedDatagramFrameCount  atomic.Uint64
	receivedDatagramByteCount   atomic.Uint64

	droppedPacketCount                             atomic.Uint64
	droppedPacketByteCount                         atomic.Uint64
	droppedDosPreventionPacketCount                atomic.Uint64
	droppedDuplicatePacketCount                    atomic.Uint64
	droppedOtherPacketCount                        atomic.Uint64
	droppedKeyUnavailablePacketCount               atomic.Uint64
	droppedUnknownConnectionIdPacketCount          atomic.Uint64
	droppedHeaderParseErrorPacketCount             atomic.Uint64
	droppedPayloadDecryptErrorPacketCount          atomic.Uint64
	droppedProtocolViolationPacketCount            atomic.Uint64
	droppedUnsupportedVersionPacketCount           atomic.Uint64
	droppedUnexpectedPacketCount                   atomic.Uint64
	droppedUnexpectedSourceConnectionIdPacketCount atomic.Uint64
	droppedUnexpectedVersionPacketCount            atomic.Uint64
	droppedPayloadDecryptBeforeKeyUpdateCount      atomic.Uint64
	droppedPayloadDecryptAfterKeyUpdateCount       atomic.Uint64
	localKeyUpdateCount                            atomic.Uint64
	remoteKeyUpdateCount                           atomic.Uint64
	keyDiscardCount                                atomic.Uint64
	lostPacketCount                                atomic.Uint64
	probeTimeoutCount                              atomic.Uint64
	handshakeAttemptCount                          atomic.Uint64
	handshakeSuccessCount                          atomic.Uint64
	handshakeFailureCount                          atomic.Uint64
	handshakeSentWithoutResponseCount              atomic.Uint64
	mtuUpdateCount                                 atomic.Uint64
	currentMtu                                     atomic.Int64
}

func (self *H3QuicPacketStats) Snapshot() H3QuicPacketStatsSnapshot {
	if self == nil {
		return H3QuicPacketStatsSnapshot{}
	}
	return H3QuicPacketStatsSnapshot{
		ConnectionCount:                                self.connectionCount.Load(),
		ClosedConnectionCount:                          self.closedConnectionCount.Load(),
		SentPacketCount:                                self.sentPacketCount.Load(),
		SentPacketByteCount:                            self.sentPacketByteCount.Load(),
		SentDatagramPacketCount:                        self.sentDatagramPacketCount.Load(),
		SentDatagramFrameCount:                         self.sentDatagramFrameCount.Load(),
		SentDatagramByteCount:                          self.sentDatagramByteCount.Load(),
		ReceivedPacketCount:                            self.receivedPacketCount.Load(),
		ReceivedPacketByteCount:                        self.receivedPacketByteCount.Load(),
		ReceivedDatagramPacketCount:                    self.receivedDatagramPacketCount.Load(),
		ReceivedDatagramFrameCount:                     self.receivedDatagramFrameCount.Load(),
		ReceivedDatagramByteCount:                      self.receivedDatagramByteCount.Load(),
		DroppedPacketCount:                             self.droppedPacketCount.Load(),
		DroppedPacketByteCount:                         self.droppedPacketByteCount.Load(),
		DroppedDosPreventionPacketCount:                self.droppedDosPreventionPacketCount.Load(),
		DroppedDuplicatePacketCount:                    self.droppedDuplicatePacketCount.Load(),
		DroppedOtherPacketCount:                        self.droppedOtherPacketCount.Load(),
		DroppedKeyUnavailablePacketCount:               self.droppedKeyUnavailablePacketCount.Load(),
		DroppedUnknownConnectionIdPacketCount:          self.droppedUnknownConnectionIdPacketCount.Load(),
		DroppedHeaderParseErrorPacketCount:             self.droppedHeaderParseErrorPacketCount.Load(),
		DroppedPayloadDecryptErrorPacketCount:          self.droppedPayloadDecryptErrorPacketCount.Load(),
		DroppedProtocolViolationPacketCount:            self.droppedProtocolViolationPacketCount.Load(),
		DroppedUnsupportedVersionPacketCount:           self.droppedUnsupportedVersionPacketCount.Load(),
		DroppedUnexpectedPacketCount:                   self.droppedUnexpectedPacketCount.Load(),
		DroppedUnexpectedSourceConnectionIdPacketCount: self.droppedUnexpectedSourceConnectionIdPacketCount.Load(),
		DroppedUnexpectedVersionPacketCount:            self.droppedUnexpectedVersionPacketCount.Load(),
		DroppedPayloadDecryptBeforeKeyUpdateCount:      self.droppedPayloadDecryptBeforeKeyUpdateCount.Load(),
		DroppedPayloadDecryptAfterKeyUpdateCount:       self.droppedPayloadDecryptAfterKeyUpdateCount.Load(),
		LocalKeyUpdateCount:                            self.localKeyUpdateCount.Load(),
		RemoteKeyUpdateCount:                           self.remoteKeyUpdateCount.Load(),
		KeyDiscardCount:                                self.keyDiscardCount.Load(),
		LostPacketCount:                                self.lostPacketCount.Load(),
		ProbeTimeoutCount:                              self.probeTimeoutCount.Load(),
		HandshakeAttemptCount:                          self.handshakeAttemptCount.Load(),
		HandshakeSuccessCount:                          self.handshakeSuccessCount.Load(),
		HandshakeFailureCount:                          self.handshakeFailureCount.Load(),
		HandshakeSentWithoutResponseCount:              self.handshakeSentWithoutResponseCount.Load(),
		MtuUpdateCount:                                 self.mtuUpdateCount.Load(),
		CurrentMtu:                                     self.currentMtu.Load(),
	}
}

// Tracer matches quic.Config.Tracer. Each returned trace reduces events into
// atomics immediately and never retains frame slices or payload bytes.
func (self *H3QuicPacketStats) Tracer(
	ctx context.Context,
	perspective bool,
	connectionID quic.ConnectionID,
) qlogwriter.Trace {
	return self.tracerForAttempt(nil)(ctx, perspective, connectionID)
}

func (self *H3QuicPacketStats) tracerForAttempt(
	attempt *h3QuicHandshakeAttempt,
) func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace {
	return func(
		_ context.Context,
		_ bool,
		_ quic.ConnectionID,
	) qlogwriter.Trace {
		self.connectionCount.Add(1)
		return &h3QuicPacketStatsTrace{stats: self, attempt: attempt}
	}
}

type h3QuicHandshakeAttempt struct {
	stats    *H3QuicPacketStats
	sent     atomic.Uint64
	received atomic.Uint64
	pto      atomic.Uint64
	finished atomic.Bool
}

func (self *H3QuicPacketStats) beginHandshakeAttempt() *h3QuicHandshakeAttempt {
	if self == nil {
		return nil
	}
	self.handshakeAttemptCount.Add(1)
	return &h3QuicHandshakeAttempt{stats: self}
}

func (self *h3QuicHandshakeAttempt) finish(success bool) {
	if self == nil || !self.finished.CompareAndSwap(false, true) {
		return
	}
	if success {
		self.stats.handshakeSuccessCount.Add(1)
		return
	}
	self.stats.handshakeFailureCount.Add(1)
	if 0 < self.sent.Load() && self.received.Load() == 0 {
		self.stats.handshakeSentWithoutResponseCount.Add(1)
	}
}

func (self *h3QuicHandshakeAttempt) sentWithoutResponse() bool {
	return self != nil && 0 < self.sent.Load() && self.received.Load() == 0
}

type h3QuicPacketStatsTrace struct {
	stats   *H3QuicPacketStats
	attempt *h3QuicHandshakeAttempt
}

func (self *h3QuicPacketStatsTrace) AddProducer() qlogwriter.Recorder {
	return &h3QuicPacketStatsRecorder{stats: self.stats, attempt: self.attempt}
}

func (self *h3QuicPacketStatsTrace) SupportsSchemas(string) bool {
	return true
}

type h3QuicPacketStatsRecorder struct {
	stats   *H3QuicPacketStats
	attempt *h3QuicHandshakeAttempt
	closed  atomic.Bool
}

func h3QuicDatagramFrames(frames []qlog.Frame) (count uint64, byteCount uint64) {
	for _, frame := range frames {
		if datagram, ok := frame.Frame.(*qlog.DatagramFrame); ok {
			count += 1
			byteCount += uint64(max(int64(0), datagram.Length))
		}
	}
	return
}

func (self *h3QuicPacketStatsRecorder) recordSent(event qlog.PacketSent) {
	self.stats.sentPacketCount.Add(1)
	if self.attempt != nil {
		self.attempt.sent.Add(1)
	}
	self.stats.sentPacketByteCount.Add(uint64(max(0, event.Raw.Length)))
	self.stats.PacketFingerprints.recordSent(event.DatagramPayloadChecksum)
	frameCount, byteCount := h3QuicDatagramFrames(event.Frames)
	if 0 < frameCount {
		self.stats.sentDatagramPacketCount.Add(1)
		self.stats.sentDatagramFrameCount.Add(frameCount)
		self.stats.sentDatagramByteCount.Add(byteCount)
	}
}

func (self *h3QuicPacketStatsRecorder) recordReceived(event qlog.PacketReceived) {
	self.stats.receivedPacketCount.Add(1)
	if self.attempt != nil {
		self.attempt.received.Add(1)
	}
	self.stats.receivedPacketByteCount.Add(uint64(max(0, event.Raw.Length)))
	self.stats.PacketFingerprints.recordReceived(event.DatagramPayloadChecksum)
	frameCount, byteCount := h3QuicDatagramFrames(event.Frames)
	if 0 < frameCount {
		self.stats.receivedDatagramPacketCount.Add(1)
		self.stats.receivedDatagramFrameCount.Add(frameCount)
		self.stats.receivedDatagramByteCount.Add(byteCount)
	}
}

func (self *h3QuicPacketStatsRecorder) recordDropped(event qlog.PacketDropped) {
	self.stats.droppedPacketCount.Add(1)
	self.stats.droppedPacketByteCount.Add(uint64(max(0, event.Raw.Length)))
	switch event.Trigger {
	case qlog.PacketDropDOSPrevention:
		self.stats.droppedDosPreventionPacketCount.Add(1)
	case qlog.PacketDropDuplicate:
		self.stats.droppedDuplicatePacketCount.Add(1)
	case qlog.PacketDropKeyUnavailable:
		self.stats.droppedOtherPacketCount.Add(1)
		self.stats.droppedKeyUnavailablePacketCount.Add(1)
	case qlog.PacketDropUnknownConnectionID:
		self.stats.droppedOtherPacketCount.Add(1)
		self.stats.droppedUnknownConnectionIdPacketCount.Add(1)
	case qlog.PacketDropHeaderParseError:
		self.stats.droppedOtherPacketCount.Add(1)
		self.stats.droppedHeaderParseErrorPacketCount.Add(1)
	case qlog.PacketDropPayloadDecryptError:
		self.stats.droppedOtherPacketCount.Add(1)
		self.stats.droppedPayloadDecryptErrorPacketCount.Add(1)
		self.stats.PacketFingerprints.recordDroppedPayloadDecrypt(
			event.DatagramPayloadChecksum,
		)
		if self.stats.localKeyUpdateCount.Load()+self.stats.remoteKeyUpdateCount.Load() == 0 {
			self.stats.droppedPayloadDecryptBeforeKeyUpdateCount.Add(1)
		} else {
			self.stats.droppedPayloadDecryptAfterKeyUpdateCount.Add(1)
		}
	case qlog.PacketDropProtocolViolation:
		self.stats.droppedOtherPacketCount.Add(1)
		self.stats.droppedProtocolViolationPacketCount.Add(1)
	case qlog.PacketDropUnsupportedVersion:
		self.stats.droppedOtherPacketCount.Add(1)
		self.stats.droppedUnsupportedVersionPacketCount.Add(1)
	case qlog.PacketDropUnexpectedPacket:
		self.stats.droppedOtherPacketCount.Add(1)
		self.stats.droppedUnexpectedPacketCount.Add(1)
	case qlog.PacketDropUnexpectedSourceConnectionID:
		self.stats.droppedOtherPacketCount.Add(1)
		self.stats.droppedUnexpectedSourceConnectionIdPacketCount.Add(1)
	case qlog.PacketDropUnexpectedVersion:
		self.stats.droppedOtherPacketCount.Add(1)
		self.stats.droppedUnexpectedVersionPacketCount.Add(1)
	default:
		self.stats.droppedOtherPacketCount.Add(1)
	}
}

func (self *h3QuicPacketStatsRecorder) recordKeyUpdated(event qlog.KeyUpdated) {
	// quic-go emits the same phase transition for the client and server 1-RTT
	// secret labels. Count one label so every actual transition appears once.
	if event.KeyType != qlog.KeyTypeClient1RTT {
		return
	}
	switch event.Trigger {
	case qlog.KeyUpdateLocal:
		self.stats.localKeyUpdateCount.Add(1)
	case qlog.KeyUpdateRemote:
		self.stats.remoteKeyUpdateCount.Add(1)
	}
}

func (self *h3QuicPacketStatsRecorder) recordKeyDiscarded(event qlog.KeyDiscarded) {
	if event.KeyType == qlog.KeyTypeClient1RTT {
		self.stats.keyDiscardCount.Add(1)
	}
}

func (self *h3QuicPacketStatsRecorder) RecordEvent(event qlogwriter.Event) {
	switch event := event.(type) {
	case qlog.PacketSent:
		self.recordSent(event)
	case *qlog.PacketSent:
		self.recordSent(*event)
	case qlog.PacketReceived:
		self.recordReceived(event)
	case *qlog.PacketReceived:
		self.recordReceived(*event)
	case qlog.PacketDropped:
		self.recordDropped(event)
	case *qlog.PacketDropped:
		self.recordDropped(*event)
	case qlog.PacketLost, *qlog.PacketLost:
		self.stats.lostPacketCount.Add(1)
	case qlog.LossTimerUpdated:
		if event.Type == qlog.LossTimerUpdateTypeExpired && event.TimerType == qlog.TimerTypePTO {
			self.stats.probeTimeoutCount.Add(1)
			if self.attempt != nil {
				self.attempt.pto.Add(1)
			}
		}
	case *qlog.LossTimerUpdated:
		if event.Type == qlog.LossTimerUpdateTypeExpired && event.TimerType == qlog.TimerTypePTO {
			self.stats.probeTimeoutCount.Add(1)
			if self.attempt != nil {
				self.attempt.pto.Add(1)
			}
		}
	case qlog.KeyUpdated:
		self.recordKeyUpdated(event)
	case *qlog.KeyUpdated:
		self.recordKeyUpdated(*event)
	case qlog.KeyDiscarded:
		self.recordKeyDiscarded(event)
	case *qlog.KeyDiscarded:
		self.recordKeyDiscarded(*event)
	case qlog.MTUUpdated:
		self.stats.mtuUpdateCount.Add(1)
		self.stats.currentMtu.Store(int64(event.Value))
	case *qlog.MTUUpdated:
		self.stats.mtuUpdateCount.Add(1)
		self.stats.currentMtu.Store(int64(event.Value))
	}
}

func (self *h3QuicPacketStatsRecorder) Close() error {
	if self.closed.CompareAndSwap(false, true) {
		self.stats.closedConnectionCount.Add(1)
	}
	return nil
}

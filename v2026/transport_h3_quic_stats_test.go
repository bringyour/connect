package connect

import (
	"context"
	"reflect"
	"testing"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
)

func TestH3QuicPacketStatsReduceFrameBoundaries(t *testing.T) {
	stats := &H3QuicPacketStats{}
	trace := stats.Tracer(context.Background(), true, quic.ConnectionID{})
	recorder := trace.AddProducer()
	recorder.RecordEvent(qlog.PacketSent{
		Raw: qlog.RawInfo{Length: 1200},
		Frames: []qlog.Frame{{
			Frame: &qlog.DatagramFrame{Length: 1100},
		}},
	})
	recorder.RecordEvent(qlog.PacketReceived{
		Raw: qlog.RawInfo{Length: 1250},
		Frames: []qlog.Frame{
			{Frame: &qlog.DatagramFrame{Length: 500}},
			{Frame: &qlog.DatagramFrame{Length: 600}},
		},
	})
	recorder.RecordEvent(qlog.PacketDropped{
		Raw:     qlog.RawInfo{Length: 88},
		Trigger: qlog.PacketDropDOSPrevention,
	})
	recorder.RecordEvent(&qlog.PacketDropped{
		Raw:     qlog.RawInfo{Length: 77},
		Trigger: qlog.PacketDropDuplicate,
	})
	recorder.RecordEvent(qlog.PacketDropped{
		Raw:     qlog.RawInfo{Length: 66},
		Trigger: qlog.PacketDropPayloadDecryptError,
	})
	recorder.RecordEvent(qlog.KeyUpdated{
		Trigger:  qlog.KeyUpdateLocal,
		KeyType:  qlog.KeyTypeClient1RTT,
		KeyPhase: 1,
	})
	recorder.RecordEvent(qlog.KeyUpdated{
		Trigger:  qlog.KeyUpdateLocal,
		KeyType:  qlog.KeyTypeServer1RTT,
		KeyPhase: 1,
	})
	recorder.RecordEvent(qlog.PacketDropped{
		Raw:     qlog.RawInfo{Length: 55},
		Trigger: qlog.PacketDropPayloadDecryptError,
	})
	recorder.RecordEvent(qlog.KeyUpdated{
		Trigger:  qlog.KeyUpdateRemote,
		KeyType:  qlog.KeyTypeClient1RTT,
		KeyPhase: 2,
	})
	recorder.RecordEvent(qlog.KeyDiscarded{
		KeyType:  qlog.KeyTypeClient1RTT,
		KeyPhase: 0,
	})
	recorder.RecordEvent(qlog.PacketLost{})
	recorder.RecordEvent(qlog.LossTimerUpdated{
		Type:      qlog.LossTimerUpdateTypeExpired,
		TimerType: qlog.TimerTypePTO,
	})
	recorder.RecordEvent(qlog.LossTimerUpdated{
		Type:      qlog.LossTimerUpdateTypeSet,
		TimerType: qlog.TimerTypePTO,
	})
	recorder.RecordEvent(qlog.LossTimerUpdated{
		Type:      qlog.LossTimerUpdateTypeExpired,
		TimerType: qlog.TimerTypeACK,
	})
	recorder.RecordEvent(qlog.MTUUpdated{Value: 1200})
	if err := recorder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Close(); err != nil {
		t.Fatal(err)
	}

	want := H3QuicPacketStatsSnapshot{
		ConnectionCount:                           1,
		ClosedConnectionCount:                     1,
		SentPacketCount:                           1,
		SentPacketByteCount:                       1200,
		SentDatagramPacketCount:                   1,
		SentDatagramFrameCount:                    1,
		SentDatagramByteCount:                     1100,
		ReceivedPacketCount:                       1,
		ReceivedPacketByteCount:                   1250,
		ReceivedDatagramPacketCount:               1,
		ReceivedDatagramFrameCount:                2,
		ReceivedDatagramByteCount:                 1100,
		DroppedPacketCount:                        4,
		DroppedPacketByteCount:                    286,
		DroppedDosPreventionPacketCount:           1,
		DroppedDuplicatePacketCount:               1,
		DroppedOtherPacketCount:                   2,
		DroppedPayloadDecryptErrorPacketCount:     2,
		DroppedPayloadDecryptBeforeKeyUpdateCount: 1,
		DroppedPayloadDecryptAfterKeyUpdateCount:  1,
		LocalKeyUpdateCount:                       1,
		RemoteKeyUpdateCount:                      1,
		KeyDiscardCount:                           1,
		LostPacketCount:                           1,
		ProbeTimeoutCount:                         1,
		MtuUpdateCount:                            1,
		CurrentMtu:                                1200,
	}
	if got := stats.Snapshot(); got != want {
		t.Fatalf("QUIC packet stats=%+v want=%+v", got, want)
	}
}

func TestH3QuicPacketStatsSeparatesPtoAndHandshakeNoResponse(t *testing.T) {
	stats := &H3QuicPacketStats{}

	failedAttempt := stats.beginHandshakeAttempt()
	failedRecorder := stats.tracerForAttempt(failedAttempt)(
		context.Background(),
		true,
		quic.ConnectionID{},
	).AddProducer()
	failedRecorder.RecordEvent(qlog.PacketSent{Raw: qlog.RawInfo{Length: 1200}})
	failedRecorder.RecordEvent(qlog.LossTimerUpdated{
		Type:      qlog.LossTimerUpdateTypeExpired,
		TimerType: qlog.TimerTypePTO,
	})
	failedRecorder.RecordEvent(qlog.PacketSent{Raw: qlog.RawInfo{Length: 1200}})
	failedAttempt.finish(false)
	failedAttempt.finish(false)

	successfulAttempt := stats.beginHandshakeAttempt()
	successfulRecorder := stats.tracerForAttempt(successfulAttempt)(
		context.Background(),
		true,
		quic.ConnectionID{},
	).AddProducer()
	successfulRecorder.RecordEvent(qlog.PacketSent{Raw: qlog.RawInfo{Length: 1200}})
	successfulRecorder.RecordEvent(qlog.PacketReceived{Raw: qlog.RawInfo{Length: 1200}})
	successfulAttempt.finish(true)

	snapshot := stats.Snapshot()
	if snapshot.ProbeTimeoutCount != 1 ||
		snapshot.HandshakeAttemptCount != 2 ||
		snapshot.HandshakeSuccessCount != 1 ||
		snapshot.HandshakeFailureCount != 1 ||
		snapshot.HandshakeSentWithoutResponseCount != 1 {
		t.Fatalf("QUIC handshake/PTO stats=%+v", snapshot)
	}
}

func TestH3QuicPacketFingerprintStatsAreBoundedAndOwned(t *testing.T) {
	fingerprints := NewH3QuicPacketFingerprintStats(2)
	stats := &H3QuicPacketStats{PacketFingerprints: fingerprints}
	recorder := stats.Tracer(context.Background(), true, quic.ConnectionID{}).AddProducer()
	for _, checksum := range []qlog.DatagramPayloadChecksum{0, 11, 11, 22, 33} {
		recorder.RecordEvent(qlog.PacketSent{DatagramPayloadChecksum: checksum})
	}
	for _, checksum := range []qlog.DatagramPayloadChecksum{11, 33} {
		recorder.RecordEvent(qlog.PacketReceived{DatagramPayloadChecksum: checksum})
	}
	for _, checksum := range []qlog.DatagramPayloadChecksum{11, 11, 44} {
		recorder.RecordEvent(qlog.PacketDropped{
			DatagramPayloadChecksum: checksum,
			Trigger:                 qlog.PacketDropPayloadDecryptError,
		})
	}
	recorder.RecordEvent(qlog.PacketDropped{
		DatagramPayloadChecksum: 55,
		Trigger:                 qlog.PacketDropDuplicate,
	})

	want := H3QuicPacketFingerprintStatsSnapshot{
		Sent:                    map[uint32]uint64{11: 2, 22: 1},
		Received:                map[uint32]uint64{11: 1, 33: 1},
		DroppedPayloadDecrypt:   map[uint32]uint64{11: 2, 44: 1},
		RefusedFingerprintCount: 1,
		UnavailableCount:        1,
	}
	got := fingerprints.Snapshot()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("QUIC fingerprints=%+v want=%+v", got, want)
	}
	got.Sent[11] = 99
	if next := fingerprints.Snapshot(); next.Sent[11] != 2 {
		t.Fatalf("snapshot mutated retained fingerprints: %+v", next)
	}
}

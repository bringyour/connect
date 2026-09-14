package connect

import (
	"context"
	"testing"
	"time"
)

func TestMultiRouteSelectorReportsActualTransport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := NewMultiRouteSelector(ctx, "writer", nil, TransferPath{}, false)
	defer writer.Close()
	sendRoute := make(Route, 1)
	writer.updateTransport(
		NewSendGatewayTransportWithType(TransportTypeH3),
		[]Route{sendRoute},
	)
	success, transportType, err := writer.WriteDetailedWithTransport(
		ctx,
		[]byte("egress"),
		time.Second,
	)
	if err != nil || !success || transportType != TransportTypeH3 {
		t.Fatalf("write = (%t, %q, %v), want (true, %q, nil)", success, transportType, err, TransportTypeH3)
	}
	<-sendRoute

	reader := NewMultiRouteSelector(ctx, "reader", nil, TransferPath{}, false)
	defer reader.Close()
	receiveRoute := make(Route, 1)
	reader.updateTransport(
		NewReceiveGatewayTransportWithType(TransportTypeH1),
		[]Route{receiveRoute},
	)
	receiveRoute <- []byte("ingress")
	message, transportType, err := reader.ReadWithTransport(ctx, time.Second)
	if err != nil || string(message) != "ingress" || transportType != TransportTypeH1 {
		t.Fatalf("read = (%q, %q, %v), want (%q, %q, nil)", message, transportType, err, "ingress", TransportTypeH1)
	}
}

func TestTransportPacketAttributionReconcilesWithAggregate(t *testing.T) {
	counters := &packetStatsCounters{}
	attribution := newTransportPacketAttribution(counters, 2, 2400)
	attribution.admit()

	stats := counters.snapshot()
	if stats.RemoteEgressPacketCount != 2 || stats.RemoteEgressByteCount != 2400 {
		t.Fatalf("unexpected aggregate before route: %+v", stats)
	}
	unknown := stats.TransportStats[TransportTypeUnknown]
	if unknown.RemoteEgressPacketCount != 2 || unknown.RemoteEgressByteCount != 2400 {
		t.Fatalf("unexpected unknown bucket before route: %+v", unknown)
	}

	attribution.observe(TransportTypeH3)
	// A duplicate race attempt cannot move the same logical traffic again.
	attribution.observe(TransportTypeH1)
	counters.recordRemoteIngress(TransportTypeH1, 3, 3600)
	stats = counters.snapshot()
	if stats.TransportStats[TransportTypeUnknown].RemoteEgressPacketCount != 0 ||
		stats.TransportStats[TransportTypeH3].RemoteEgressPacketCount != 2 ||
		stats.TransportStats[TransportTypeH1].RemoteIngressPacketCount != 3 {
		t.Fatalf("unexpected attributed stats: %+v", stats.TransportStats)
	}

	var egressPackets int64
	var egressBytes ByteCount
	var ingressPackets int64
	var ingressBytes ByteCount
	for _, transportType := range TransportTypes() {
		transportStats := stats.TransportStats[transportType]
		egressPackets += transportStats.RemoteEgressPacketCount
		egressBytes += transportStats.RemoteEgressByteCount
		ingressPackets += transportStats.RemoteIngressPacketCount
		ingressBytes += transportStats.RemoteIngressByteCount
	}
	if egressPackets != stats.RemoteEgressPacketCount ||
		egressBytes != stats.RemoteEgressByteCount ||
		ingressPackets != stats.RemoteIngressPacketCount ||
		ingressBytes != stats.RemoteIngressByteCount {
		t.Fatalf(
			"transport sum (%d, %d, %d, %d) != aggregate (%d, %d, %d, %d)",
			egressPackets,
			egressBytes,
			ingressPackets,
			ingressBytes,
			stats.RemoteEgressPacketCount,
			stats.RemoteEgressByteCount,
			stats.RemoteIngressPacketCount,
			stats.RemoteIngressByteCount,
		)
	}
}

func TestSendGroupTransportObserverFiresOnceAcrossChunks(t *testing.T) {
	var observed []TransportType
	completion := newSendGroupCompletion(
		&SendPack{transportWriteObserver: func(transportType TransportType) {
			observed = append(observed, transportType)
		}},
		2,
	)
	first := completion.chunkAckRecord()
	second := completion.chunkAckRecord()
	first.observeTransportWrite(TransportTypeH3)
	second.observeTransportWrite(TransportTypeH1)
	if len(observed) != 1 || observed[0] != TransportTypeH3 {
		t.Fatalf("observed = %v, want [h3]", observed)
	}
}

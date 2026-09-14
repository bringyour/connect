package connect

// These tests pin the H3 DATAGRAM envelope's fragmentation, memory, duplicate,
// expiry, and corruption boundaries independently of a real QUIC socket.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/urnetwork/connect/v2026/protocol"
)

func TestH3HybridStreamSendBudgetBoundsRetainedBytesAndWakes(t *testing.T) {
	stats := &H3DatagramStats{}
	budget := NewH3HybridStreamSendBudget(3, 32, stats)
	if !budget.Acquire(t.Context(), 24) || !budget.Acquire(t.Context(), 8) {
		t.Fatal("initial queue reservations failed")
	}
	before := stats.Snapshot()
	if before.HybridStreamQueueCurrentMessageCount != 2 ||
		before.HybridStreamQueueCurrentByteCount != 32 ||
		before.HybridStreamQueueMaximumMessageCount != 2 ||
		before.HybridStreamQueueMaximumByteCount != 32 {
		t.Fatalf("full queue stats=%+v", before)
	}

	attempted := make(chan struct{})
	acquired := make(chan bool, 1)
	go func() {
		close(attempted)
		acquired <- budget.Acquire(t.Context(), 1)
	}()
	<-attempted
	select {
	case result := <-acquired:
		t.Fatalf("byte-bound admission completed before release: %t", result)
	case <-time.After(25 * time.Millisecond):
	}

	budget.Release(24)
	select {
	case result := <-acquired:
		if !result {
			t.Fatal("released byte budget did not wake admission")
		}
	case <-time.After(time.Second):
		t.Fatal("released byte budget did not wake waiter")
	}
	afterWake := stats.Snapshot()
	if afterWake.HybridStreamQueueCurrentMessageCount != 2 ||
		afterWake.HybridStreamQueueCurrentByteCount != 9 ||
		afterWake.HybridStreamQueueMaximumByteCount != 32 ||
		afterWake.HybridStreamQueueWaitCount != 1 ||
		afterWake.HybridStreamQueueWaitDuration <= 0 {
		t.Fatalf("woken queue stats=%+v", afterWake)
	}
	budget.Release(8)
	budget.Release(1)
	final := stats.Snapshot()
	if final.HybridStreamQueueCurrentMessageCount != 0 ||
		final.HybridStreamQueueCurrentByteCount != 0 {
		t.Fatalf("released queue stats=%+v", final)
	}
}

func TestH3HybridStreamSendBudgetBoundsCountAndCancellation(t *testing.T) {
	stats := &H3DatagramStats{}
	budget := NewH3HybridStreamSendBudget(1, 64, stats)
	if !budget.Acquire(t.Context(), 8) {
		t.Fatal("initial count reservation failed")
	}
	waitCtx, cancelWait := context.WithCancel(t.Context())
	attempted := make(chan struct{})
	finished := make(chan bool, 1)
	go func() {
		close(attempted)
		finished <- budget.Acquire(waitCtx, 8)
	}()
	<-attempted
	cancelWait()
	select {
	case result := <-finished:
		if result {
			t.Fatal("cancelled count-bound admission succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled count-bound admission did not return")
	}
	budget.Release(8)
	snapshot := stats.Snapshot()
	if snapshot.HybridStreamQueueCurrentMessageCount != 0 ||
		snapshot.HybridStreamQueueCurrentByteCount != 0 ||
		snapshot.HybridStreamQueueMaximumMessageCount != 1 ||
		snapshot.HybridStreamQueueWaitCount != 1 {
		t.Fatalf("cancelled queue stats=%+v", snapshot)
	}
}

func TestH3HybridStreamSendBudgetRejectsOversizedBackingAllocation(t *testing.T) {
	stats := &H3DatagramStats{}
	budget := NewH3HybridStreamSendBudget(2, 32, stats)
	if budget.Acquire(t.Context(), 33) {
		t.Fatal("oversized backing allocation entered bounded queue")
	}
	snapshot := stats.Snapshot()
	if snapshot.HybridStreamQueueOversizeCount != 1 ||
		snapshot.HybridStreamQueueCurrentMessageCount != 0 ||
		snapshot.HybridStreamQueueCurrentByteCount != 0 {
		t.Fatalf("oversized queue stats=%+v", snapshot)
	}

	message := MessagePoolGet(1)
	defer MessagePoolReturn(message)
	if retained := H3HybridStreamRetainedByteCount(message); retained != cap(message) ||
		retained <= len(message) {
		t.Fatalf("pooled retained bytes=%d len=%d cap=%d", retained, len(message), cap(message))
	}
}

func TestH3HybridStreamDefaultBudgetFitsEveryPacketPoolSlot(t *testing.T) {
	if H3HybridStreamQueueByteCount !=
		64*1024+H3HybridStreamQueueMessageCount*MessagePoolMetaByteCount {
		t.Fatalf("default hybrid stream byte limit=%d", H3HybridStreamQueueByteCount)
	}
	stats := &H3DatagramStats{}
	budget := NewH3HybridStreamSendBudget(
		H3HybridStreamQueueMessageCount,
		H3HybridStreamQueueByteCount,
		stats,
	)
	messages := make([][]byte, 0, H3HybridStreamQueueMessageCount)
	for range H3HybridStreamQueueMessageCount {
		message := MessagePoolGet(packetPoolSize)
		if !budget.Acquire(t.Context(), H3HybridStreamRetainedByteCount(message)) {
			MessagePoolReturn(message)
			for _, acquired := range messages {
				budget.Release(H3HybridStreamRetainedByteCount(acquired))
				MessagePoolReturn(acquired)
			}
			t.Fatalf("packet-pool slot %d exceeded default byte limit", len(messages)+1)
		}
		messages = append(messages, message)
	}
	snapshot := stats.Snapshot()
	if snapshot.HybridStreamQueueMaximumMessageCount != H3HybridStreamQueueMessageCount ||
		snapshot.HybridStreamQueueMaximumByteCount != H3HybridStreamQueueByteCount {
		t.Fatalf("default full queue stats=%+v", snapshot)
	}
	for _, message := range messages {
		budget.Release(H3HybridStreamRetainedByteCount(message))
		MessagePoolReturn(message)
	}
	final := stats.Snapshot()
	if final.HybridStreamQueueCurrentMessageCount != 0 ||
		final.HybridStreamQueueCurrentByteCount != 0 {
		t.Fatalf("default released queue stats=%+v", final)
	}
}

func TestH3InitialDatagramPathByteCountUsesSynchronousQuicLimit(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	probeByteCount := 0
	maximum := initialH3DatagramPathByteCount(
		settings.TargetDatagramByteCount,
		func(probe []byte) error {
			probeByteCount = len(probe)
			return &quic.DatagramTooLargeError{
				MaxDatagramPayloadSize: H3InitialDatagramByteCount,
			}
		},
	)
	if probeByteCount != 2048 || maximum != H3InitialDatagramByteCount {
		t.Fatalf("probe bytes/maximum = %d/%d", probeByteCount, maximum)
	}

	clamped := initialH3DatagramPathByteCount(
		settings.TargetDatagramByteCount,
		func([]byte) error {
			return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: 4096}
		},
	)
	if clamped != settings.TargetDatagramByteCount {
		t.Fatalf("reported jumbo limit was not clamped: %d", clamped)
	}

	fallback := initialH3DatagramPathByteCount(
		settings.TargetDatagramByteCount,
		func([]byte) error { return errors.New("no size feedback") },
	)
	if fallback != H3InitialDatagramByteCount {
		t.Fatalf("non-size fallback = %d, want %d", fallback, H3InitialDatagramByteCount)
	}
}

// Hybrid selection follows the connection's current payload and bounded
// fragment ceilings.
func TestH3HybridDatagramSelectionUsesLivePathLimit(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	if settings.TargetDatagramByteCount != 1360 ||
		settings.HybridDatagramMessageByteCount != 1332 ||
		settings.MaxFragmentCount != 1 {
		t.Fatalf("default H3 DATAGRAM sizing=%+v", settings)
	}
	if !settings.UseDatagram(1332) || settings.UseDatagram(1333) {
		t.Fatal("configured hybrid-message boundary was not enforced")
	}
	const reducedDatagramByteCount = 1200
	if settings.UseDatagramForPath(
		settings.HybridDatagramMessageByteCount,
		reducedDatagramByteCount,
	) {
		t.Fatal("reduced path fragmented a message instead of selecting stream")
	}
	fragmented := *settings
	fragmented.MaxFragmentCount = 2
	if !fragmented.UseDatagramForPath(
		fragmented.HybridDatagramMessageByteCount,
		reducedDatagramByteCount,
	) {
		t.Fatal("explicit two-fragment control did not retain the message")
	}
	if settings.UseDatagramForPath(
		settings.HybridDatagramMessageByteCount+1,
		reducedDatagramByteCount,
	) {
		t.Fatal("reduced path selected a frame above the hybrid threshold")
	}
	if settings.UseDatagramForPath(1, H3DatagramHeaderByteCount) {
		t.Fatal("header-only path selected DATAGRAM")
	}
}

func TestH3DatagramTransferFrameLimitMatchesLaneSelection(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		fragmentCount int
		pathByteCount int
	}{
		{name: "default path", fragmentCount: 1, pathByteCount: H3InitialDatagramByteCount},
		{name: "reduced path", fragmentCount: 1, pathByteCount: 1200},
		{name: "bounded fragmentation", fragmentCount: 2, pathByteCount: 1200},
	} {
		settings := *DefaultH3DatagramSettings()
		settings.MaxFragmentCount = testCase.fragmentCount
		limit := H3DatagramTransferFrameByteLimit(&settings, testCase.pathByteCount)
		if limit <= 0 {
			t.Fatalf("%s serialized DATAGRAM limit=%d", testCase.name, limit)
		}
		for byteCount := 1; byteCount <= settings.MaxMessageByteCount; byteCount++ {
			wantDatagram := settings.UseDatagramForPath(byteCount, testCase.pathByteCount)
			if gotDatagram := byteCount <= limit; gotDatagram != wantDatagram {
				t.Fatalf(
					"%s bytes=%d serialized DATAGRAM=%t live selection=%t limit=%d",
					testCase.name,
					byteCount,
					gotDatagram,
					wantDatagram,
					limit,
				)
			}
		}
	}
}

func TestH3HybridDatagramPathReductionFallsBackPastFragmentLimit(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	stats := &H3DatagramStats{}
	fragmenter, err := NewH3DatagramFragmenter(settings, stats)
	if err != nil {
		t.Fatal(err)
	}
	// One DATAGRAM on the configured path, but beyond the bounded fragment count
	// after the peer reports a much smaller live payload.
	message := bytes.Repeat([]byte{0x5a}, 1200)
	sendCount := 0
	useStream, nextMax, err := fragmenter.SendHybrid(
		message,
		settings.TargetDatagramByteCount,
		func([]byte) error {
			sendCount += 1
			return &quic.DatagramTooLargeError{
				MaxDatagramPayloadSize: H3DatagramHeaderByteCount + 100,
			}
		},
	)
	if err != nil || !useStream ||
		nextMax != H3DatagramHeaderByteCount+100 || sendCount != 1 {
		t.Fatalf(
			"hybrid fallback stream=%t max=%d sends=%d err=%v",
			useStream,
			nextMax,
			sendCount,
			err,
		)
	}
	if snapshot := stats.Snapshot(); snapshot.SentFragmentCount != 0 ||
		snapshot.SentMessageCount != 0 || snapshot.SendErrorCount != 0 {
		t.Fatalf("hybrid fallback stats=%+v", snapshot)
	}
}

func TestH3HybridDatagramKeepsContractControlOnStream(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	stats := &H3DatagramStats{}
	fragmenter, err := NewH3DatagramFragmenter(settings, stats)
	if err != nil {
		t.Fatal(err)
	}
	// This is the exact size observed for a full-TUN contract-only Pack. It has
	// no application frame and remains on the reliable control/large-message
	// stream; splitting it across two lossy datagrams failed route readiness in
	// the one-bar benchmark.
	message := bytes.Repeat([]byte{0x4f}, 1515)
	sendCount := 0
	useStream, nextMax, err := fragmenter.SendHybrid(
		message,
		settings.TargetDatagramByteCount,
		func([]byte) error {
			sendCount += 1
			return nil
		},
	)
	if err != nil || !useStream || nextMax != settings.TargetDatagramByteCount ||
		sendCount != 0 {
		t.Fatalf(
			"hybrid contract stream=%t max=%d sends=%d err=%v",
			useStream,
			nextMax,
			sendCount,
			err,
		)
	}
	if snapshot := stats.Snapshot(); snapshot != (H3DatagramStatsSnapshot{}) {
		t.Fatalf("hybrid contract stats=%+v", snapshot)
	}
}

func TestH3HybridDatagramPathReductionRetriesOneCompleteDatagram(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	stats := &H3DatagramStats{}
	fragmenter, err := NewH3DatagramFragmenter(settings, stats)
	if err != nil {
		t.Fatal(err)
	}
	message := bytes.Repeat([]byte{0x6b}, 1100)
	var datagramByteCounts []int
	useStream, nextMax, err := fragmenter.SendHybrid(
		message,
		settings.TargetDatagramByteCount,
		func(datagram []byte) error {
			datagramByteCounts = append(datagramByteCounts, len(datagram))
			if len(datagramByteCounts) == 1 {
				return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: 1200}
			}
			return nil
		},
	)
	if err != nil || useStream || nextMax != 1200 ||
		len(datagramByteCounts) != 2 ||
		datagramByteCounts[0] != H3DatagramHeaderByteCount+len(message) ||
		datagramByteCounts[1] != H3DatagramHeaderByteCount+len(message) {
		t.Fatalf(
			"hybrid retry stream=%t max=%d datagrams=%v err=%v",
			useStream,
			nextMax,
			datagramByteCounts,
			err,
		)
	}
	if snapshot := stats.Snapshot(); snapshot.SentFragmentCount != 1 ||
		snapshot.SentMessageCount != 1 || snapshot.SendErrorCount != 0 {
		t.Fatalf("hybrid retry stats=%+v", snapshot)
	}
}

func TestH3HybridDatagramPathReductionRetriesTwoFragments(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	// Fragmentation remains supported for an explicit compatibility or
	// experiment setting even though production's hybrid limit is one.
	settings.MaxFragmentCount = 2
	stats := &H3DatagramStats{}
	fragmenter, err := NewH3DatagramFragmenter(settings, stats)
	if err != nil {
		t.Fatal(err)
	}
	message := bytes.Repeat([]byte{0x2c}, 1288)
	var datagramByteCounts []int
	useStream, nextMax, err := fragmenter.SendHybrid(
		message,
		settings.TargetDatagramByteCount,
		func(datagram []byte) error {
			datagramByteCounts = append(datagramByteCounts, len(datagram))
			if len(datagramByteCounts) == 1 {
				return &quic.DatagramTooLargeError{
					MaxDatagramPayloadSize: H3InitialDatagramByteCount,
				}
			}
			return nil
		},
	)
	if err != nil || useStream || nextMax != H3InitialDatagramByteCount ||
		len(datagramByteCounts) != 3 ||
		datagramByteCounts[0] != H3DatagramHeaderByteCount+len(message) ||
		datagramByteCounts[1] != H3InitialDatagramByteCount ||
		datagramByteCounts[2] != H3DatagramHeaderByteCount+
			len(message)-(H3InitialDatagramByteCount-H3DatagramHeaderByteCount) {
		t.Fatalf(
			"hybrid fragmented retry stream=%t max=%d datagrams=%v err=%v",
			useStream,
			nextMax,
			datagramByteCounts,
			err,
		)
	}
	if snapshot := stats.Snapshot(); snapshot.SentFragmentCount != 2 ||
		snapshot.SentMessageCount != 1 || snapshot.SendErrorCount != 0 {
		t.Fatalf("hybrid fragmented retry stats=%+v", snapshot)
	}
}

// Overlapping coverage is rejected even when every declared fragment index is
// present and the individual payload slices remain in bounds.
func TestH3DatagramRejectsOverlappingFragments(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	settings.MaxFragmentCount = 2
	datagrams, _ := captureH3Datagrams(
		t,
		settings,
		[]byte("overlap-must-not-reassemble"),
		H3DatagramHeaderByteCount+14,
	)
	if len(datagrams) != 2 {
		t.Fatalf("fragments=%d want=2", len(datagrams))
	}
	// Move the second fragment one byte backward without changing its index.
	binary.BigEndian.PutUint32(datagrams[1][20:24], 13)
	budget := NewH3DatagramReassemblyBudget(settings.ProcessReassemblyByteCount)
	stats := &H3DatagramStats{}
	reassembler, err := NewH3DatagramReassembler(settings, budget, stats)
	if err != nil {
		t.Fatal(err)
	}
	for _, datagram := range datagrams {
		if message := reassembler.Accept(datagram, time.Unix(50, 0)); message != nil {
			MessagePoolReturn(message)
			t.Fatal("overlapping fragments delivered")
		}
	}
	reassembler.Close()
	if budget.Used() != 0 {
		t.Fatalf("overlap retained %d shared bytes", budget.Used())
	}
	if snapshot := stats.Snapshot(); snapshot.MalformedFragmentCount != 1 {
		t.Fatalf("stats=%+v", snapshot)
	}
}

// Truncation, unsupported fragment counts, and attacker-controlled total sizes
// are rejected before any shared-byte reservation or pooled allocation.
func TestH3DatagramRejectsInvalidDeclarationsBeforeAllocation(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	valid, _ := captureH3Datagrams(t, settings, []byte("valid"), settings.TargetDatagramByteCount)
	truncated := bytes.Clone(valid[0][:H3DatagramHeaderByteCount])
	tooManyFragments := bytes.Clone(valid[0])
	binary.BigEndian.PutUint16(tooManyFragments[26:28], uint16(settings.MaxFragmentCount+1))
	oversized := bytes.Clone(valid[0])
	binary.BigEndian.PutUint32(oversized[12:16], uint32(settings.MaxMessageByteCount+1))

	budget := NewH3DatagramReassemblyBudget(settings.ProcessReassemblyByteCount)
	stats := &H3DatagramStats{}
	reassembler, err := NewH3DatagramReassembler(settings, budget, stats)
	if err != nil {
		t.Fatal(err)
	}
	for _, datagram := range [][]byte{truncated, tooManyFragments, oversized} {
		if message := reassembler.Accept(datagram, time.Unix(75, 0)); message != nil {
			MessagePoolReturn(message)
			t.Fatal("invalid declaration delivered")
		}
	}
	reassembler.Close()
	if budget.Used() != 0 {
		t.Fatalf("invalid declarations reserved %d shared bytes", budget.Used())
	}
	if snapshot := stats.Snapshot(); snapshot.MalformedFragmentCount != 3 {
		t.Fatalf("stats=%+v", snapshot)
	}
}

// New endpoints select DATAGRAM only when both QUIC transport parameters and
// the authenticated application envelope version agree.
func TestH3DatagramAuthNegotiatesNewPeers(t *testing.T) {
	request := &protocol.Auth{ByJwt: "jwt", AppVersion: "1.2.3", InstanceId: []byte("instance")}
	SetH3DatagramAuthOffer(request, true)
	response, serverAccepted := AcceptH3DatagramAuthOffer(request, true, true, true)
	if !serverAccepted {
		t.Fatal("server did not accept matching capabilities")
	}
	clientAccepted, err := ValidateH3DatagramAuthResponse(request, response, true, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if !clientAccepted {
		t.Fatal("client did not accept matching capabilities")
	}
}

// An old server's exact auth echo contains the client's offer as an unknown
// field but no separate acceptance, so the live connection safely stays on the
// reliable stream without reconnecting.
func TestH3DatagramAuthFallsBackForLegacyServer(t *testing.T) {
	request := &protocol.Auth{ByJwt: "jwt", AppVersion: "1.2.3", InstanceId: []byte("instance")}
	SetH3DatagramAuthOffer(request, true)
	legacyEcho := &protocol.Auth{
		ByJwt:             request.ByJwt,
		AppVersion:        request.AppVersion,
		InstanceId:        bytes.Clone(request.InstanceId),
		H3DatagramVersion: request.H3DatagramVersion,
	}
	accepted, err := ValidateH3DatagramAuthResponse(request, legacyEcho, true, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if accepted {
		t.Fatal("legacy server echo selected DATAGRAM")
	}
}

// A server setting, either endpoint's QUIC transport parameter, or a different
// envelope offer independently keeps the legacy stream mode.
func TestH3DatagramAuthRequiresEveryCapability(t *testing.T) {
	request := &protocol.Auth{ByJwt: "jwt", AppVersion: "1.2.3", InstanceId: []byte("instance")}
	SetH3DatagramAuthOffer(request, true)
	cases := []struct {
		enabled bool
		local   bool
		remote  bool
	}{
		{enabled: false, local: true, remote: true},
		{enabled: true, local: false, remote: true},
		{enabled: true, local: true, remote: false},
	}
	for _, c := range cases {
		response, accepted := AcceptH3DatagramAuthOffer(request, c.enabled, c.local, c.remote)
		if accepted || response.H3DatagramAcceptedVersion != 0 {
			t.Fatalf("enabled=%t local=%t remote=%t response=%+v accepted=%t", c.enabled, c.local, c.remote, response, accepted)
		}
	}
	request.H3DatagramVersion = H3DatagramProtocolVersion + 1
	if response, accepted := AcceptH3DatagramAuthOffer(request, true, true, true); accepted || response.H3DatagramAcceptedVersion != 0 {
		t.Fatalf("unknown offer response=%+v accepted=%t", response, accepted)
	}
}

// Capability fields never weaken the existing auth echo: identity changes and
// an unoffered nonzero acceptance are protocol errors.
func TestH3DatagramAuthRejectsMismatchedResponse(t *testing.T) {
	request := &protocol.Auth{ByJwt: "jwt", AppVersion: "1.2.3", InstanceId: []byte("instance")}
	SetH3DatagramAuthOffer(request, true)
	response, _ := AcceptH3DatagramAuthOffer(request, true, true, true)
	response.InstanceId = []byte("other")
	if _, err := ValidateH3DatagramAuthResponse(request, response, true, true, true); err == nil {
		t.Fatal("identity mismatch accepted")
	}
	response.InstanceId = bytes.Clone(request.InstanceId)
	response.H3DatagramAcceptedVersion = H3DatagramProtocolVersion + 1
	if _, err := ValidateH3DatagramAuthResponse(request, response, true, true, true); err == nil {
		t.Fatal("unknown accepted version accepted")
	}
}

// Captures copied datagrams because the production fragmenter deliberately
// reuses one scratch buffer after each synchronous quic-go send.
func captureH3Datagrams(
	t *testing.T,
	settings *H3DatagramSettings,
	message []byte,
	maxDatagramByteCount int,
) ([][]byte, *H3DatagramStats) {
	t.Helper()
	stats := &H3DatagramStats{}
	fragmenter, err := NewH3DatagramFragmenter(settings, stats)
	if err != nil {
		t.Fatal(err)
	}
	var datagrams [][]byte
	_, err = fragmenter.Send(message, maxDatagramByteCount, func(datagram []byte) error {
		datagrams = append(datagrams, bytes.Clone(datagram))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return datagrams, stats
}

// Every exact payload boundary must produce the minimum fragment count and
// reassemble to the original complete Transfer frame.
func TestH3DatagramFragmentBoundariesRoundTrip(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	settings.MaxFragmentCount = 8
	maxDatagramByteCount := H3DatagramHeaderByteCount + 16
	messageByteCounts := []int{1, 16, 17, 31, 32, 33, 127}
	for _, messageByteCount := range messageByteCounts {
		message := make([]byte, messageByteCount)
		for i := range message {
			message[i] = byte(i*29 + 7)
		}
		datagrams, sendStats := captureH3Datagrams(t, settings, message, maxDatagramByteCount)
		wantFragmentCount := (messageByteCount + 15) / 16
		if len(datagrams) != wantFragmentCount {
			t.Fatalf("message bytes=%d fragments=%d want=%d", messageByteCount, len(datagrams), wantFragmentCount)
		}
		budget := NewH3DatagramReassemblyBudget(settings.ProcessReassemblyByteCount)
		receiveStats := &H3DatagramStats{}
		reassembler, err := NewH3DatagramReassembler(settings, budget, receiveStats)
		if err != nil {
			t.Fatal(err)
		}
		var received []byte
		for i := len(datagrams) - 1; 0 <= i; i -= 1 {
			if message := reassembler.Accept(datagrams[i], time.Unix(100, 0)); message != nil {
				if received != nil {
					t.Fatalf("message bytes=%d delivered more than once", messageByteCount)
				}
				received = message
			}
		}
		if !bytes.Equal(received, message) {
			t.Fatalf("message bytes=%d received=%x want=%x", messageByteCount, received, message)
		}
		MessagePoolReturn(received)
		reassembler.Close()
		if budget.Used() != 0 {
			t.Fatalf("message bytes=%d retained shared bytes=%d", messageByteCount, budget.Used())
		}
		if snapshot := sendStats.Snapshot(); snapshot.SentMessageCount != 1 || snapshot.SentFragmentCount != uint64(wantFragmentCount) {
			t.Fatalf("message bytes=%d send stats=%+v", messageByteCount, snapshot)
		}
		if snapshot := receiveStats.Snapshot(); snapshot.ReceivedMessageCount != 1 || snapshot.ReceivedFragmentCount != uint64(wantFragmentCount) {
			t.Fatalf("message bytes=%d receive stats=%+v", messageByteCount, snapshot)
		}
	}
}

// A duplicate fragment is ignored and does not move the first fragment's hard
// expiry deadline.
func TestH3DatagramDuplicateDoesNotExtendReassemblyLifetime(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	settings.MaxFragmentCount = 2
	settings.ReassemblyTimeout = 5 * time.Second
	datagrams, _ := captureH3Datagrams(
		t,
		settings,
		[]byte("fragmented-message"),
		H3DatagramHeaderByteCount+10,
	)
	budget := NewH3DatagramReassemblyBudget(settings.ProcessReassemblyByteCount)
	stats := &H3DatagramStats{}
	reassembler, err := NewH3DatagramReassembler(settings, budget, stats)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(200, 0)
	if message := reassembler.Accept(datagrams[0], start); message != nil {
		t.Fatal("incomplete message delivered")
	}
	if message := reassembler.Accept(datagrams[0], start.Add(4*time.Second)); message != nil {
		t.Fatal("duplicate fragment delivered")
	}
	reassembler.Expire(start.Add(5 * time.Second))
	if budget.Used() != 0 {
		t.Fatalf("expired message retained %d shared bytes", budget.Used())
	}
	for _, datagram := range datagrams[1:] {
		if message := reassembler.Accept(datagram, start.Add(5*time.Second)); message != nil {
			t.Fatal("tail fragments completed an expired generation")
		}
	}
	if budget.Used() != 0 {
		t.Fatalf("late fragments resurrected %d shared bytes", budget.Used())
	}
	reassembler.Close()
	snapshot := stats.Snapshot()
	if snapshot.DuplicateFragmentCount != 2 || snapshot.ReassemblyTimeoutCount != 1 {
		t.Fatalf("stats=%+v", snapshot)
	}
}

// A completed carrier message id is remembered within a bounded replay window,
// preventing duplicated QUIC datagrams from redelivering the Transfer frame.
func TestH3DatagramCompletedMessageDuplicateIsNotRedelivered(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	datagrams, _ := captureH3Datagrams(t, settings, []byte("one fragment"), settings.TargetDatagramByteCount)
	budget := NewH3DatagramReassemblyBudget(settings.ProcessReassemblyByteCount)
	stats := &H3DatagramStats{}
	reassembler, err := NewH3DatagramReassembler(settings, budget, stats)
	if err != nil {
		t.Fatal(err)
	}
	message := reassembler.Accept(datagrams[0], time.Unix(300, 0))
	if !bytes.Equal(message, []byte("one fragment")) {
		t.Fatalf("received=%q", message)
	}
	MessagePoolReturn(message)
	if duplicate := reassembler.Accept(datagrams[0], time.Unix(301, 0)); duplicate != nil {
		MessagePoolReturn(duplicate)
		t.Fatal("completed message duplicate was redelivered")
	}
	reassembler.Close()
	if snapshot := stats.Snapshot(); snapshot.ReceivedMessageCount != 1 || snapshot.DuplicateFragmentCount != 1 {
		t.Fatalf("stats=%+v", snapshot)
	}
}

// Inconsistent metadata retires the incomplete id immediately, and corrupted
// payload bytes cannot pass the whole-message checksum.
func TestH3DatagramRejectsInconsistentMetadataAndChecksum(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	settings.MaxFragmentCount = 2
	datagrams, _ := captureH3Datagrams(
		t,
		settings,
		[]byte("two-fragment-checksum"),
		H3DatagramHeaderByteCount+12,
	)
	budget := NewH3DatagramReassemblyBudget(settings.ProcessReassemblyByteCount)
	stats := &H3DatagramStats{}
	reassembler, err := NewH3DatagramReassembler(settings, budget, stats)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(400, 0)
	reassembler.Accept(datagrams[0], start)
	inconsistent := bytes.Clone(datagrams[1])
	inconsistent[19] ^= 0x01
	if message := reassembler.Accept(inconsistent, start); message != nil {
		MessagePoolReturn(message)
		t.Fatal("inconsistent metadata delivered")
	}
	if budget.Used() != 0 {
		t.Fatalf("inconsistent message retained %d shared bytes", budget.Used())
	}
	reassembler.Close()
	reassembler, err = NewH3DatagramReassembler(settings, budget, stats)
	if err != nil {
		t.Fatal(err)
	}

	corruptDatagrams, _ := captureH3Datagrams(
		t,
		settings,
		[]byte("another-checksum-message"),
		H3DatagramHeaderByteCount+12,
	)
	corruptDatagrams[len(corruptDatagrams)-1][H3DatagramHeaderByteCount] ^= 0xff
	for _, datagram := range corruptDatagrams {
		if message := reassembler.Accept(datagram, start); message != nil {
			MessagePoolReturn(message)
			t.Fatal("checksum-corrupt message delivered")
		}
	}
	reassembler.Close()
	if budget.Used() != 0 {
		t.Fatalf("corrupt message retained %d shared bytes", budget.Used())
	}
	snapshot := stats.Snapshot()
	if snapshot.MalformedFragmentCount != 1 || snapshot.ChecksumFailureCount != 1 {
		t.Fatalf("stats=%+v", snapshot)
	}
}

// Two peers share one hard process budget: one incomplete frame can reserve it,
// the other is refused without allocating, and close makes the bytes reusable.
func TestH3DatagramSharedReassemblyBudgetRecoversAfterClose(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	settings.MaxFragmentCount = 2
	settings.MaxMessageByteCount = 64
	settings.MaxReassemblyByteCount = 64
	settings.ProcessReassemblyByteCount = 64
	datagramsA, _ := captureH3Datagrams(t, settings, bytes.Repeat([]byte{0xa1}, 64), H3DatagramHeaderByteCount+32)
	datagramsB, _ := captureH3Datagrams(t, settings, bytes.Repeat([]byte{0xb2}, 64), H3DatagramHeaderByteCount+32)
	budget := NewH3DatagramReassemblyBudget(settings.ProcessReassemblyByteCount)
	stats := &H3DatagramStats{}
	reassemblerA, err := NewH3DatagramReassembler(settings, budget, stats)
	if err != nil {
		t.Fatal(err)
	}
	reassemblerB, err := NewH3DatagramReassembler(settings, budget, stats)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(500, 0)
	reassemblerA.Accept(datagramsA[0], now)
	if budget.Used() != 64 {
		t.Fatalf("reserved bytes=%d want=64", budget.Used())
	}
	if message := reassemblerB.Accept(datagramsB[0], now); message != nil {
		MessagePoolReturn(message)
		t.Fatal("second peer bypassed shared budget")
	}
	reassemblerA.Close()
	if budget.Used() != 0 {
		t.Fatalf("close retained %d shared bytes", budget.Used())
	}
	var received []byte
	for _, datagram := range datagramsB {
		if message := reassemblerB.Accept(datagram, now); message != nil {
			received = message
		}
	}
	if !bytes.Equal(received, bytes.Repeat([]byte{0xb2}, 64)) {
		t.Fatalf("received=%x", received)
	}
	MessagePoolReturn(received)
	reassemblerB.Close()
	if budget.Used() != 0 {
		t.Fatalf("completion retained %d shared bytes", budget.Used())
	}
	if snapshot := stats.Snapshot(); snapshot.ReassemblyLimitCount != 1 {
		t.Fatalf("stats=%+v", snapshot)
	}
}

// An oversized message and a synchronous sender refusal are both terminal for
// that carrier attempt, with no unbounded retry inside the fragmenter.
func TestH3DatagramFragmenterBoundsAndSendFailure(t *testing.T) {
	settings := DefaultH3DatagramSettings()
	settings.MaxMessageByteCount = 32
	stats := &H3DatagramStats{}
	fragmenter, err := NewH3DatagramFragmenter(settings, stats)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fragmenter.Send(bytes.Repeat([]byte{1}, 33), settings.TargetDatagramByteCount, func([]byte) error { return nil }); !errors.Is(err, ErrH3DatagramMessageTooLarge) {
		t.Fatalf("oversized error=%v", err)
	}
	wantErr := errors.New("synthetic send refusal")
	if sent, err := fragmenter.Send([]byte("valid"), settings.TargetDatagramByteCount, func([]byte) error { return wantErr }); sent != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("sent=%d error=%v", sent, err)
	}
	if snapshot := stats.Snapshot(); snapshot.SendErrorCount != 2 || snapshot.SentMessageCount != 0 {
		t.Fatalf("stats=%+v", snapshot)
	}
}

// Arbitrary authenticated input must remain bounded and panic-free. Valid seed
// envelopes also exercise state creation and pooled-buffer cleanup.
func FuzzH3DatagramReassembler(f *testing.F) {
	settings := DefaultH3DatagramSettings()
	datagrams, _ := captureH3DatagramsForFuzz(settings, []byte("valid fragmented seed"), H3DatagramHeaderByteCount+8)
	f.Add([]byte{})
	f.Add([]byte("not an envelope"))
	for _, datagram := range datagrams {
		f.Add(datagram)
	}
	f.Fuzz(func(t *testing.T, datagram []byte) {
		budget := NewH3DatagramReassemblyBudget(settings.ProcessReassemblyByteCount)
		reassembler, err := NewH3DatagramReassembler(settings, budget, &H3DatagramStats{})
		if err != nil {
			t.Fatal(err)
		}
		if message := reassembler.Accept(datagram, time.Unix(600, 0)); message != nil {
			MessagePoolReturn(message)
		}
		reassembler.Close()
		if budget.Used() != 0 {
			t.Fatalf("retained shared bytes=%d", budget.Used())
		}
	})
}

// Provides fuzz seeds without a testing.T helper dependency.
func captureH3DatagramsForFuzz(
	settings *H3DatagramSettings,
	message []byte,
	maxDatagramByteCount int,
) ([][]byte, error) {
	fragmenter, err := NewH3DatagramFragmenter(settings, &H3DatagramStats{})
	if err != nil {
		return nil, err
	}
	var datagrams [][]byte
	_, err = fragmenter.Send(message, maxDatagramByteCount, func(datagram []byte) error {
		datagrams = append(datagrams, bytes.Clone(datagram))
		return nil
	})
	return datagrams, err
}

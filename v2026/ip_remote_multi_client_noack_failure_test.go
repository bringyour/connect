package connect

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// Builds a two-packet UDP group with packet and byte accounting that can be
// checked independently after completion.
func newPacketTransferTestGroup() *parsedPacketGroup {
	udpPath := udpTestPath(4)
	return &parsedPacketGroup{
		packets: []parsedPacket{
			{packet: make([]byte, 1000), ipPath: udpPath},
			{packet: make([]byte, 1100), ipPath: udpPath},
		},
		ipPath:    udpPath,
		byteCount: 2100,
	}
}

// A direct P2P UDP packet uses Transfer NoAck. Its terminal callback is the
// initial route-write disposition, so a transient Timeout is one dropped
// datagram, not a verdict on every TCP flow sharing the provider channel.
func TestNoAckWriteTimeoutDoesNotPoisonSharedTcpClient(t *testing.T) {
	client := newPacketTransferTestChannel()
	tcpBytes := ByteCount(1440)
	udpBytes := ByteCount(1000)
	client.sendTransferForTest = func(ackCallback AckFunction) (bool, error) {
		ackCallback(errTransferRouteWriteTimeout)
		return true, nil
	}

	client.addSend(tcpBytes, icmpTcpTestPath(4))
	success, err := client.SendDetailedWithAck(&parsedPacket{
		packet: make([]byte, udpBytes),
		ipPath: udpTestPath(4),
	}, time.Second, false)
	if err != nil || !success {
		t.Fatalf("NoAck send = %t, %v; want admitted before its route-write timeout", success, err)
	}

	client.stateLock.Lock()
	defer client.stateLock.Unlock()
	if client.endErr != nil {
		t.Fatalf("NoAck timeout poisoned provider: %v", client.endErr)
	}
	if got := client.packetStats.sendNackCount; got != 1 {
		t.Fatalf("outstanding packets = %d, want only the TCP packet", got)
	}
	if got := client.packetStats.sendNackByteCount; got != tcpBytes {
		t.Fatalf("outstanding bytes = %d, want TCP bytes %d", got, tcpBytes)
	}
	if got := client.packetStats.sendAckCount; got != 0 {
		t.Fatalf("ack credit = %d, want 0 for a failed NoAck write", got)
	}
}

// Provider-return and coalesced reads can admit several UDP packets as one
// logical group. The same timeout boundary must retire every group member
// without resetting an unrelated reliable flow.
func TestNoAckGroupWriteTimeoutDoesNotPoisonSharedTcpClient(t *testing.T) {
	client := newPacketTransferTestChannel()
	tcpBytes := ByteCount(1440)
	group := newPacketTransferTestGroup()
	client.sendTransferForTest = func(ackCallback AckFunction) (bool, error) {
		ackCallback(errTransferRouteWriteTimeout)
		return true, nil
	}

	client.addSend(tcpBytes, icmpTcpTestPath(4))
	success, err := client.SendGroupDetailedWithAck(group, time.Second, false)
	if err != nil || !success {
		t.Fatalf("NoAck group = %t, %v; want admitted before its route-write timeout", success, err)
	}

	client.stateLock.Lock()
	defer client.stateLock.Unlock()
	if client.endErr != nil {
		t.Fatalf("NoAck group timeout poisoned provider: %v", client.endErr)
	}
	if got := client.packetStats.sendNackCount; got != 1 {
		t.Fatalf("outstanding packets = %d, want only the TCP packet", got)
	}
	if got := client.packetStats.sendNackByteCount; got != tcpBytes {
		t.Fatalf("outstanding bytes = %d, want TCP bytes %d", got, tcpBytes)
	}
	if got := client.packetStats.sendAckCount; got != 0 {
		t.Fatalf("ack credit = %d, want 0 for a failed NoAck group write", got)
	}
}

// A group shares the singleton structural boundary: NoAck weakens only the
// typed route-write timeout, not sequence, contract, or encryption failures.
func TestNoAckGroupStructuralFailureStillPoisonsProvider(t *testing.T) {
	client := newPacketTransferTestChannel()
	wantErr := errors.New("Send sequence closed.")
	client.sendTransferForTest = func(ackCallback AckFunction) (bool, error) {
		ackCallback(wantErr)
		return true, nil
	}

	group := newPacketTransferTestGroup()
	success, err := client.SendGroupDetailedWithAck(group, time.Second, false)
	if err != nil || !success {
		t.Fatalf("NoAck group = %t, %v; want admitted before its structural failure", success, err)
	}

	client.stateLock.Lock()
	defer client.stateLock.Unlock()
	if !errors.Is(client.endErr, wantErr) {
		t.Fatalf("provider error = %v, want %v", client.endErr, wantErr)
	}
	if got := client.packetStats.sendNackCount; got != len(group.packets) {
		t.Fatalf("structurally failed group outstanding count = %d, want %d", got, len(group.packets))
	}
	if got := client.packetStats.sendNackByteCount; got != group.byteCount {
		t.Fatalf("structurally failed group outstanding bytes = %d, want %d", got, group.byteCount)
	}
	if got := client.packetStats.sendAckCount; got != 0 {
		t.Fatalf("structurally failed group ack credit = %d, want 0", got)
	}
}

// NoAck does not make structural failures harmless. A closed sequence,
// contract failure, or encryption failure still carries provider-level
// evidence; only the typed bounded route-write timeout is packet-local.
func TestNoAckStructuralFailureStillPoisonsProvider(t *testing.T) {
	client := newPacketTransferTestChannel()
	udpBytes := ByteCount(1000)
	wantErr := errors.New("Send sequence closed.")
	client.sendTransferForTest = func(ackCallback AckFunction) (bool, error) {
		ackCallback(wantErr)
		return true, nil
	}

	success, err := client.SendDetailedWithAck(&parsedPacket{
		packet: make([]byte, udpBytes),
		ipPath: udpTestPath(4),
	}, time.Second, false)
	if err != nil || !success {
		t.Fatalf("NoAck send = %t, %v; want admitted before its structural failure", success, err)
	}

	client.stateLock.Lock()
	defer client.stateLock.Unlock()
	if !errors.Is(client.endErr, wantErr) {
		t.Fatalf("provider error = %v, want %v", client.endErr, wantErr)
	}
	if got := client.packetStats.sendNackCount; got != 1 {
		t.Fatalf("structurally failed send outstanding count = %d, want 1", got)
	}
	if got := client.packetStats.sendNackByteCount; got != udpBytes {
		t.Fatalf("structurally failed send outstanding bytes = %d, want %d", got, udpBytes)
	}
	if got := client.packetStats.sendAckCount; got != 0 {
		t.Fatalf("structurally failed send ack credit = %d, want 0", got)
	}
}

// Ack-required failures retain their old meaning: Transfer exhausted reliable
// recovery, which is hard provider evidence and must still wake removal.
func TestAckFailureStillPoisonsProvider(t *testing.T) {
	for _, wantErr := range []error{
		errTransferRouteWriteTimeout,
		errors.New("Send sequence closed."),
	} {
		client := newPacketTransferTestChannel()
		tcpBytes := ByteCount(1440)
		client.sendTransferForTest = func(ackCallback AckFunction) (bool, error) {
			ackCallback(wantErr)
			return true, nil
		}

		success, err := client.SendDetailedWithAck(&parsedPacket{
			packet: make([]byte, tcpBytes),
			ipPath: icmpTcpTestPath(4),
		}, time.Second, true)
		if err != nil || !success {
			t.Fatalf("Ack send before %v = %t, %v; want true, nil", wantErr, success, err)
		}

		client.stateLock.Lock()
		providerErr := client.endErr
		sendNackCount := client.packetStats.sendNackCount
		sendNackByteCount := client.packetStats.sendNackByteCount
		sendAckCount := client.packetStats.sendAckCount
		client.stateLock.Unlock()
		if !errors.Is(providerErr, wantErr) {
			t.Fatalf("provider error = %v, want %v", providerErr, wantErr)
		}
		if sendNackCount != 1 {
			t.Fatalf("reliable outstanding packets after %v = %d, want 1", wantErr, sendNackCount)
		}
		if sendNackByteCount != tcpBytes {
			t.Fatalf("reliable outstanding bytes after %v = %d, want %d", wantErr, sendNackByteCount, tcpBytes)
		}
		if sendAckCount != 0 {
			t.Fatalf("failed reliable send ack credit after %v = %d, want 0", wantErr, sendAckCount)
		}
	}
}

// The real singleton and group callbacks must each publish one packet-local
// timeout disposition even if competing terminal paths invoke them together.
// This also behaviorally pins both hot paths to the NoAck classifier above.
func TestPacketTransferCallbacksUseNoAckFailureClassifier(t *testing.T) {
	const completionAttempts = 16
	completeConcurrently := func(ackCallback AckFunction) {
		start := make(chan struct{})
		var waitGroup sync.WaitGroup
		waitGroup.Add(completionAttempts)
		for range completionAttempts {
			go func() {
				defer waitGroup.Done()
				<-start
				ackCallback(errTransferRouteWriteTimeout)
			}()
		}
		close(start)
		waitGroup.Wait()
	}

	packetClient := newPacketTransferTestChannel()
	packetClient.sendTransferForTest = func(ackCallback AckFunction) (bool, error) {
		completeConcurrently(ackCallback)
		return true, nil
	}
	packetSuccess, packetErr := packetClient.SendDetailedWithAck(&parsedPacket{
		packet: make([]byte, 1000),
		ipPath: udpTestPath(4),
	}, time.Second, false)
	if packetErr != nil || !packetSuccess {
		t.Fatalf("singleton NoAck send = %t, %v; want true, nil", packetSuccess, packetErr)
	}
	packetClient.stateLock.Lock()
	packetNackCount := packetClient.packetStats.sendNackCount
	packetNackByteCount := packetClient.packetStats.sendNackByteCount
	packetAckCount := packetClient.packetStats.sendAckCount
	packetEndErr := packetClient.endErr
	packetClient.stateLock.Unlock()
	if packetNackCount != 0 || packetNackByteCount != 0 || packetAckCount != 0 || packetEndErr != nil {
		t.Errorf(
			"singleton completion = nack:%d/%dB ack:%d err:%v; want one exact abandonment",
			packetNackCount,
			packetNackByteCount,
			packetAckCount,
			packetEndErr,
		)
	}

	groupClient := newPacketTransferTestChannel()
	groupClient.sendTransferForTest = func(ackCallback AckFunction) (bool, error) {
		completeConcurrently(ackCallback)
		return true, nil
	}
	group := newPacketTransferTestGroup()
	groupSuccess, groupErr := groupClient.SendGroupDetailedWithAck(group, time.Second, false)
	if groupErr != nil || !groupSuccess {
		t.Fatalf("group NoAck send = %t, %v; want true, nil", groupSuccess, groupErr)
	}
	groupClient.stateLock.Lock()
	groupNackCount := groupClient.packetStats.sendNackCount
	groupNackByteCount := groupClient.packetStats.sendNackByteCount
	groupAckCount := groupClient.packetStats.sendAckCount
	groupEndErr := groupClient.endErr
	groupClient.stateLock.Unlock()
	if groupNackCount != 0 || groupNackByteCount != 0 || groupAckCount != 0 || groupEndErr != nil {
		t.Errorf(
			"group completion = nack:%d/%dB ack:%d err:%v; want one exact abandonment",
			groupNackCount,
			groupNackByteCount,
			groupAckCount,
			groupEndErr,
		)
	}
}

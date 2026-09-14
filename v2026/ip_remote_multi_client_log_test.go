package connect

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

type sparsePacketLogTestLogger struct {
	Logger
	mu       sync.Mutex
	messages []string
}

func (self *sparsePacketLogTestLogger) Infof(format string, args ...any) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.messages = append(self.messages, fmt.Sprintf(format, args...))
}

func (self *sparsePacketLogTestLogger) snapshot() []string {
	self.mu.Lock()
	defer self.mu.Unlock()
	return append([]string(nil), self.messages...)
}

type failingEgressSecurityPolicy struct {
	stats *SecurityPolicyStatsCollector
}

func (self *failingEgressSecurityPolicy) Stats() *SecurityPolicyStatsCollector {
	return self.stats
}

func (self *failingEgressSecurityPolicy) InspectEgress(
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultDrop, errors.New("policy failure")
}

func (self *failingEgressSecurityPolicy) InspectIngress(
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *failingEgressSecurityPolicy) RefreshEgress(ipPath *IpPath) {
}

func (self *failingEgressSecurityPolicy) RefreshIngress(ipPath *IpPath) {
}

func TestMultiClientUnsupportedPacketLoggingIsSparse(t *testing.T) {
	log := &sparsePacketLogTestLogger{Logger: NewNoopLogger()}
	multi := &RemoteUserNatMultiClient{
		ctx: context.Background(),
		log: log,
	}
	// gre: a genuinely unsupported protocol (icmp echo parses since ICMP.md)
	packet := make([]byte, 20)
	packet[0] = 0x45
	packet[2] = 0
	packet[3] = byte(len(packet))
	packet[8] = 64
	packet[9] = 47

	for i := 0; i < 16; i++ {
		if multi.SendPacket(TransferPath{}, protocol.ProvideMode_Network, packet, 0) {
			t.Fatal("unsupported protocol packet was accepted")
		}
	}

	if count := multi.sendParseDropCount.Load(); count != 16 {
		t.Fatalf("parse drop count = %d, want 16", count)
	}
	messages := log.snapshot()
	if len(messages) != 5 {
		t.Fatalf("log message count = %d, want 5 at counts 1, 2, 4, 8, and 16", len(messages))
	}
	if message := messages[len(messages)-1]; !strings.Contains(message, "parse; count=16") ||
		!strings.Contains(message, "No support for protocol 47") {
		t.Fatalf("last sparse summary = %q", message)
	}
}

func TestMultiClientPolicyFailureLoggingIsSparseAndIndependent(t *testing.T) {
	log := &sparsePacketLogTestLogger{Logger: NewNoopLogger()}
	multi := &RemoteUserNatMultiClient{
		ctx:            context.Background(),
		log:            log,
		securityPolicy: &failingEgressSecurityPolicy{stats: DefaultSecurityPolicyStatsCollector()},
	}
	packet := testingUdp4Packet("10.0.0.1", "203.0.113.7", 443, []byte("payload"))

	for i := 0; i < 8; i++ {
		if multi.SendPacket(TransferPath{}, protocol.ProvideMode_Network, nil, 0) {
			t.Fatal("empty packet was accepted")
		}
		if multi.SendPacket(TransferPath{}, protocol.ProvideMode_Network, packet, 0) {
			t.Fatal("packet rejected by the security policy was accepted")
		}
	}

	if count := multi.sendParseDropCount.Load(); count != 8 {
		t.Fatalf("parse drop count = %d, want 8", count)
	}
	if count := multi.sendPolicyDropCount.Load(); count != 8 {
		t.Fatalf("policy drop count = %d, want 8", count)
	}
	messages := log.snapshot()
	if len(messages) != 8 {
		t.Fatalf("log message count = %d, want 8 for two independent counters", len(messages))
	}
	if !strings.Contains(messages[len(messages)-1], "policy; count=8") {
		t.Fatalf("last sparse summary = %q, want policy count 8", messages[len(messages)-1])
	}
}

func TestMultiClientUnsupportedPacketLoggingIsSparseUnderConcurrency(t *testing.T) {
	log := &sparsePacketLogTestLogger{Logger: NewNoopLogger()}
	multi := &RemoteUserNatMultiClient{
		ctx: context.Background(),
		log: log,
	}

	const packetCount = 128
	var wait sync.WaitGroup
	wait.Add(packetCount)
	for i := 0; i < packetCount; i++ {
		go func() {
			defer wait.Done()
			multi.SendPacket(TransferPath{}, protocol.ProvideMode_Network, nil, 0)
		}()
	}
	wait.Wait()

	if count := multi.sendParseDropCount.Load(); count != packetCount {
		t.Fatalf("parse drop count = %d, want %d", count, packetCount)
	}
	messages := log.snapshot()
	if len(messages) != 8 {
		t.Fatalf("log message count = %d, want 8 through count 128", len(messages))
	}
}

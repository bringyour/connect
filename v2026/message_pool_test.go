package connect

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	mathrand "math/rand"
	"testing"
)

func TestMessagePoolReadAllLimit(t *testing.T) {
	const limit = 8192

	exact := bytes.Repeat([]byte{0x5a}, limit)
	message, err := MessagePoolReadAllLimit(bytes.NewReader(exact), limit)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message, exact) {
		t.Fatal("exact-limit message changed")
	}
	MessagePoolReturn(message)

	message, err = MessagePoolReadAllLimit(bytes.NewReader(append(exact, 0x01)), limit)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("oversized message error = %v, want %v", err, ErrMessageTooLarge)
	}
	if message != nil {
		t.Fatal("oversized read returned a buffer")
	}
}

func TestMessagePool(t *testing.T) {
	ResetMessagePoolStats()
	for n := range 1024 * 8 {
		if n%32 == 0 {
			fmt.Printf("mem[%d]\n", n)
		}
		for range 128 {
			message := make([]byte, n)
			mathrand.Read(message)

			messageCopy := MessagePoolCopy(message)
			AssertEqual(t, len(messageCopy), n)
			AssertEqual(t, message, messageCopy)

			MessagePoolReturn(messageCopy)
		}
	}
	for n := range 1024 * 32 {
		if n%32 == 0 {
			fmt.Printf("memr[%d]\n", n)
		}
		b := make([]byte, mathrand.Intn(32*1024))
		mathrand.Read(b)
		bCopy, err := MessagePoolReadAll(bytes.NewReader(b))
		AssertEqual(t, err, nil)
		AssertEqual(t, b, bCopy)
		MessagePoolReturn(bCopy)
	}
	stats := MessagePoolStats()
	for _, tagRatios := range stats {
		for _, ratio := range tagRatios {
			AssertEqual(t, ratio, float32(1.0))
		}
	}
}

func TestMessagePoolShare(t *testing.T) {
	holdCount := 16
	holdMessages := make([][][]byte, holdCount)

	for range 1024 {
		message := MessagePoolGet(mathrand.Intn(4096))
		pooled, shared := MessagePoolCheck(message)
		AssertEqual(t, pooled, true)
		AssertEqual(t, shared, false)
		holdMessages[0] = append(holdMessages[0], message)
		k := mathrand.Intn(holdCount)
		for i := 1; i < k; i += 1 {
			MessagePoolShareReadOnly(message)
			pooled, shared = MessagePoolCheck(message)
			AssertEqual(t, pooled, true)
			AssertEqual(t, shared, true)
			holdMessages[i] = append(holdMessages[i], message)
		}
	}

	// exercise shares across every pooled size class
	for range 1024 {
		message := MessagePoolGet(mathrand.Intn(32 * 1024))
		pooled, shared := MessagePoolCheck(message)
		AssertEqual(t, pooled, len(message) <= 8192)
		AssertEqual(t, shared, false)
		k := mathrand.Intn(holdCount)
		for i := 1; i < k; i += 1 {
			MessagePoolShareReadOnly(message)
			pooled, shared = MessagePoolCheck(message)
			AssertEqual(t, pooled, len(message) <= 8192)
			AssertEqual(t, shared, len(message) <= 8192)
		}
		for i := 1; i < k; i += 1 {
			MessagePoolReturn(message)
			pooled, shared = MessagePoolCheck(message)
			AssertEqual(t, pooled, len(message) <= 8192)
			AssertEqual(t, shared, len(message) <= 8192)
		}
		MessagePoolReturn(message)
		pooled, shared = MessagePoolCheck(message)
		AssertEqual(t, pooled, false)
		AssertEqual(t, shared, false)
	}

	for i := holdCount - 1; 1 <= i; i -= 1 {
		for _, message := range holdMessages[i] {
			pooled, shared := MessagePoolCheck(message)
			AssertEqual(t, pooled, len(message) <= 8192)
			AssertEqual(t, shared, len(message) <= 8192)
			r := MessagePoolReturn(message)
			AssertEqual(t, r, false)
		}
	}
	for _, message := range holdMessages[0] {
		r := MessagePoolReturn(message)
		AssertEqual(t, r, true)
		pooled, shared := MessagePoolCheck(message)
		AssertEqual(t, pooled, false)
		AssertEqual(t, shared, false)
	}
}

func TestMessagePoolPacketOutstandingCountTracksRootOwnershipWithoutAllocating(t *testing.T) {
	baseline := MessagePoolPacketOutstandingCount()
	baselineBytes := MessagePoolPacketOutstandingByteCount()
	message := MessagePoolGet(DefaultMtu)
	if got := MessagePoolPacketOutstandingCount(); got != baseline+1 {
		t.Fatalf("packet outstanding after take = %d, want %d", got, baseline+1)
	}
	if got := MessagePoolPacketOutstandingByteCount(); got != baselineBytes+packetPoolSize {
		t.Fatalf("packet outstanding bytes after take = %d, want %d", got, baselineBytes+packetPoolSize)
	}
	if got := MessagePoolPacketRootByteCount(message); got != packetPoolSize {
		t.Fatalf("full packet root bytes = %d, want %d", got, packetPoolSize)
	}

	MessagePoolShareReadOnly(message)
	if got := MessagePoolPacketOutstandingCount(); got != baseline+1 {
		t.Fatalf("packet outstanding after share = %d, want %d", got, baseline+1)
	}
	if MessagePoolReturn(message) {
		t.Fatal("non-final shared return unexpectedly released packet root")
	}
	if got := MessagePoolPacketOutstandingCount(); got != baseline+1 {
		t.Fatalf("packet outstanding after non-final return = %d, want %d", got, baseline+1)
	}
	if !MessagePoolReturn(message) {
		t.Fatal("final shared return did not release packet root")
	}
	if got := MessagePoolPacketOutstandingCount(); got != baseline {
		t.Fatalf("packet outstanding after final return = %d, want %d", got, baseline)
	}
	if got := MessagePoolPacketOutstandingByteCount(); got != baselineBytes {
		t.Fatalf("packet outstanding bytes after final return = %d, want %d", got, baselineBytes)
	}

	if allocations := testing.AllocsPerRun(100, func() {
		_ = MessagePoolPacketOutstandingCount()
		_ = MessagePoolPacketOutstandingByteCount()
	}); allocations != 0 {
		t.Fatalf("packet outstanding snapshots allocated %.0f objects, want 0", allocations)
	}
}

func TestMessagePoolSmallPacketClassUsesByteSizedRoot(t *testing.T) {
	baselineCount := MessagePoolPacketOutstandingCount()
	baselineBytes := MessagePoolPacketOutstandingByteCount()
	message := MessagePoolGet(80)
	if got := cap(message); got != smallPacketPoolSize+MessagePoolMetaByteCount {
		t.Fatalf("small packet capacity = %d, want %d", got, smallPacketPoolSize+MessagePoolMetaByteCount)
	}
	if got := MessagePoolPacketOutstandingCount(); got != baselineCount+1 {
		t.Fatalf("small packet outstanding = %d, want %d", got, baselineCount+1)
	}
	if got := MessagePoolPacketOutstandingByteCount(); got != baselineBytes+smallPacketPoolSize {
		t.Fatalf("small packet outstanding bytes = %d, want %d", got, baselineBytes+smallPacketPoolSize)
	}
	if got := MessagePoolPacketRootByteCount(message); got != smallPacketPoolSize {
		t.Fatalf("small packet root bytes = %d, want %d", got, smallPacketPoolSize)
	}
	if !MessagePoolReturn(message) {
		t.Fatal("small packet did not return to its pool")
	}
	if got := MessagePoolPacketOutstandingByteCount(); got != baselineBytes {
		t.Fatalf("small packet bytes after return = %d, want %d", got, baselineBytes)
	}

	full := MessagePoolGet(smallPacketPoolSize + 1)
	defer MessagePoolReturn(full)
	if got := MessagePoolPacketRootByteCount(full); got != packetPoolSize {
		t.Fatalf("post-small packet root bytes = %d, want %d", got, packetPoolSize)
	}
	if got := MessagePoolPacketRootByteCount(make([]byte, 80)); got != 0 {
		t.Fatalf("unpooled packet root bytes = %d, want 0", got)
	}
}

func TestMessagePoolOutstandingSurvivesDiagnosticReset(t *testing.T) {
	if !messagePoolTrackPacketOutstanding {
		t.Skip("fast packet-root tracking is compiled only on mobile")
	}
	pool := newMessagePool(smallPacketPoolSize, messagePoolShardCount)
	message := pool.take(80, 7)
	pool.resetStats()
	shard, _ := pool.shardFor(message[:cap(message)])
	shard.stateLock.Lock()
	outstanding := shard.outstanding
	shard.stateLock.Unlock()
	if outstanding != 1 {
		t.Fatalf("outstanding after diagnostic reset = %d, want 1", outstanding)
	}
	if !pool.release(message[:cap(message)]) {
		t.Fatal("final return did not release reset-spanning root")
	}
	shard.stateLock.Lock()
	outstanding = shard.outstanding
	shard.stateLock.Unlock()
	if outstanding != 0 {
		t.Fatalf("outstanding after return = %d, want 0", outstanding)
	}
}

func TestMessagePoolRootByteCountChargesBackingClass(t *testing.T) {
	for _, testCase := range []struct {
		name string
		size int
		want ByteCount
	}{
		{name: "small packet", size: 60, want: 256},
		{name: "full packet", size: 1500, want: 2048},
		{name: "small frame", size: 3000, want: 4096},
		{name: "large frame", size: 6000, want: 8192},
		{name: "unpooled", size: 9000, want: 9000},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			message := MessagePoolGet(testCase.size)
			if got := MessagePoolRootByteCount(message); got != testCase.want {
				t.Fatalf("root charge = %d, want %d", got, testCase.want)
			}
			MessagePoolReturn(message)
		})
	}
}

func TestMessagePoolDeviceTunEgressClassificationFollowsRootLifetime(t *testing.T) {
	baseline := MessagePoolDeviceTunEgressOutstandingByteCount()
	small := MessagePoolGet(80)
	full := MessagePoolGet(DefaultMtu)
	if !MessagePoolMarkDeviceTunEgress(small) ||
		!MessagePoolMarkDeviceTunEgress(full) {
		t.Fatal("pooled packet roots rejected device TUN classification")
	}
	if !MessagePoolMarkDeviceTunEgress(small) {
		t.Fatal("idempotent device TUN classification failed")
	}
	want := baseline + smallPacketPoolSize + packetPoolSize
	if got := MessagePoolDeviceTunEgressOutstandingByteCount(); got != want {
		t.Fatalf("device TUN egress bytes = %d, want %d", got, want)
	}

	MessagePoolShareReadOnly(full)
	if MessagePoolReturn(full) {
		t.Fatal("non-final shared return released device TUN root")
	}
	if got := MessagePoolDeviceTunEgressOutstandingByteCount(); got != want {
		t.Fatalf("device TUN bytes after shared return = %d, want %d", got, want)
	}
	if !MessagePoolReturn(full) {
		t.Fatal("final shared return did not release device TUN root")
	}
	if got := MessagePoolDeviceTunEgressOutstandingByteCount(); got != baseline+smallPacketPoolSize {
		t.Fatalf("device TUN bytes after full return = %d", got)
	}
	MessagePoolReturn(small)
	if got := MessagePoolDeviceTunEgressOutstandingByteCount(); got != baseline {
		t.Fatalf("device TUN bytes after all returns = %d, want %d", got, baseline)
	}
	if MessagePoolMarkDeviceTunEgress(make([]byte, 80)) {
		t.Fatal("unpooled packet accepted device TUN classification")
	}
}

func TestBase64(t *testing.T) {
	for range 128 {
		n := mathrand.Intn(512)
		b := make([]byte, n)
		mathrand.Read(b)
		b2, err := DecodeBase64(base64.StdEncoding, EncodeBase64(base64.StdEncoding, b))
		AssertEqual(t, err, nil)
		AssertEqual(t, b, b2)
	}
}

func BenchmarkMessagePoolGetReturn(b *testing.B) {
	for _, size := range []int{80, DefaultMtu, 3000, 6000} {
		b.Run(fmt.Sprintf("serial/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				message := MessagePoolGet(size)
				message[0] = 1
				MessagePoolReturn(message)
			}
		})
		b.Run(fmt.Sprintf("parallel/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					message := MessagePoolGet(size)
					message[0] = 1
					MessagePoolReturn(message)
				}
			})
		})
	}
}

func BenchmarkMessagePoolPacketOutstandingCount(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = MessagePoolPacketOutstandingCount()
	}
}

func BenchmarkMessagePoolPacketOutstandingFastSnapshot(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _ = messagePoolPacketOutstandingSnapshot(true)
	}
}

func BenchmarkMessagePoolDeviceTunEgressLifecycle(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(80)
	for b.Loop() {
		message := MessagePoolGet(80)
		MessagePoolMarkDeviceTunEgress(message)
		MessagePoolReturn(message)
	}
}

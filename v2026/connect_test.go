package connect

import (
	// "os"
	"bytes"
	"flag"
	"testing"
)

func init() {
	initGlog()
	DebugTransferCopyOnWrite = true
}

func initGlog() {
	flag.Set("logtostderr", "true")
	flag.Set("stderrthreshold", "INFO")
	flag.Set("v", "0")
}

func TestTransferPath(t *testing.T) {
	a := NewId()
	b := NewId()
	c := NewId()

	var path TransferPath
	var err error

	path, err = TransferPathFromBytes(nil, nil, nil)
	AssertEqual(t, err, nil)
	AssertEqual(t, path, TransferPath{})
	AssertEqual(t, path.IsStream(), false)
	AssertEqual(t, path.IsSourceMask(), true)
	AssertEqual(t, path.IsDestinationMask(), true)
	AssertEqual(t, path.IsLocalMask(), true)
	AssertEqual(t, path.IsControlSource(), true)
	AssertEqual(t, path.IsControlDestination(), true)

	path, err = TransferPathFromBytes(a.Bytes(), nil, nil)
	AssertEqual(t, err, nil)
	AssertEqual(t, path, TransferPath{SourceId: a})
	AssertEqual(t, path.IsStream(), false)
	AssertEqual(t, path.IsSourceMask(), true)
	AssertEqual(t, path.IsDestinationMask(), false)
	AssertEqual(t, path.IsLocalMask(), true)
	AssertEqual(t, path.IsControlSource(), false)
	AssertEqual(t, path.IsControlDestination(), false)

	path = SourceId(a)
	AssertEqual(t, path, TransferPath{SourceId: a})
	AssertEqual(t, path.IsStream(), false)
	AssertEqual(t, path.IsSourceMask(), true)
	AssertEqual(t, path.IsDestinationMask(), false)
	AssertEqual(t, path.IsLocalMask(), true)
	AssertEqual(t, path.IsControlSource(), false)
	AssertEqual(t, path.IsControlDestination(), false)

	path, err = TransferPathFromBytes(a.Bytes(), b.Bytes(), nil)
	AssertEqual(t, err, nil)
	AssertEqual(t, path, TransferPath{SourceId: a, DestinationId: b})
	AssertEqual(t, path.IsStream(), false)
	AssertEqual(t, path.IsSourceMask(), false)
	AssertEqual(t, path.IsDestinationMask(), false)
	AssertEqual(t, path.IsLocalMask(), true)
	AssertEqual(t, path.IsControlSource(), false)
	AssertEqual(t, path.IsControlDestination(), false)

	path, err = TransferPathFromBytes(nil, b.Bytes(), nil)
	AssertEqual(t, err, nil)
	AssertEqual(t, path, TransferPath{DestinationId: b})
	AssertEqual(t, path.IsStream(), false)
	AssertEqual(t, path.IsSourceMask(), false)
	AssertEqual(t, path.IsDestinationMask(), true)
	AssertEqual(t, path.IsLocalMask(), true)
	AssertEqual(t, path.IsControlSource(), false)
	AssertEqual(t, path.IsControlDestination(), false)

	path = DestinationId(b)
	AssertEqual(t, path, TransferPath{DestinationId: b})
	AssertEqual(t, path.IsStream(), false)
	AssertEqual(t, path.IsSourceMask(), false)
	AssertEqual(t, path.IsDestinationMask(), true)
	AssertEqual(t, path.IsLocalMask(), true)
	AssertEqual(t, path.IsControlSource(), false)
	AssertEqual(t, path.IsControlDestination(), false)

	path, err = TransferPathFromBytes(a.Bytes(), b.Bytes(), c.Bytes())
	AssertEqual(t, err, nil)
	AssertNotEqual(t, path, TransferPath{StreamId: c})
	AssertEqual(t, path.IsStream(), true)
	AssertEqual(t, path.IsSourceMask(), false)
	AssertEqual(t, path.IsDestinationMask(), false)
	AssertEqual(t, path.IsLocalMask(), false)
	AssertEqual(t, path.IsControlSource(), false)
	AssertEqual(t, path.IsControlDestination(), false)

	path, err = TransferPathFromBytes(nil, nil, c.Bytes())
	AssertEqual(t, err, nil)
	AssertEqual(t, path, TransferPath{StreamId: c})
	AssertEqual(t, path.IsStream(), true)
	AssertEqual(t, path.IsSourceMask(), true)
	AssertEqual(t, path.IsDestinationMask(), true)
	AssertEqual(t, path.IsLocalMask(), false)
	AssertEqual(t, path.IsControlSource(), false)
	AssertEqual(t, path.IsControlDestination(), false)

	path = StreamId(c)
	AssertEqual(t, path, TransferPath{StreamId: c})
	AssertEqual(t, path.IsStream(), true)
	AssertEqual(t, path.IsSourceMask(), true)
	AssertEqual(t, path.IsDestinationMask(), true)
	AssertEqual(t, path.IsLocalMask(), false)
	AssertEqual(t, path.IsControlSource(), false)
	AssertEqual(t, path.IsControlDestination(), false)

	AssertEqual(t, path.Reverse(), TransferPath{StreamId: c})

	path = NewTransferPath(a, b, Id{})
	AssertEqual(t, path.IsSourceMask(), false)
	AssertEqual(t, path.IsDestinationMask(), false)
	AssertEqual(t, path.IsLocalMask(), true)
	s := path.SourceMask()
	AssertEqual(t, s.IsSourceMask(), true)
	AssertEqual(t, s.IsDestinationMask(), false)
	AssertEqual(t, s.IsLocalMask(), true)
	d := path.DestinationMask()
	AssertEqual(t, d.IsSourceMask(), false)
	AssertEqual(t, d.IsDestinationMask(), true)
	AssertEqual(t, d.IsLocalMask(), true)

	AssertEqual(t, path.Reverse(), TransferPath{SourceId: b, DestinationId: a})
}

func TestMultiHopId(t *testing.T) {
	ids := []Id{
		NewId(),
		NewId(),
		NewId(),
	}

	m, err := NewMultiHopId(ids...)
	AssertEqual(t, err, nil)
	m2 := RequireMultiHopId(ids...)
	AssertEqual(t, m, m2)
	AssertEqual(t, m.Len(), 3)
	AssertEqual(t, len(m.Ids()), 3)
	AssertEqual(t, m.Ids()[0], ids[0])
	AssertEqual(t, m.Ids()[1], ids[1])
	AssertEqual(t, m.Ids()[2], ids[2])
	AssertEqual(t, len(m.Bytes()), 3)
	AssertEqual(t, m.Bytes()[0], ids[0].Bytes())
	AssertEqual(t, m.Bytes()[1], ids[1].Bytes())
	AssertEqual(t, m.Bytes()[2], ids[2].Bytes())

	AssertEqual(t, m.Tail(), ids[2])

	m3, tail := m.SplitTail()
	m4 := RequireMultiHopId(ids[0], ids[1])
	AssertEqual(t, tail, ids[2])
	AssertEqual(t, m3, m4)
}

// The public limit counts intermediaries; the final destination occupies one
// additional slot without changing shorter-path serialization.
func TestMultiHopIdSupportsMaximumIntermediariesAndDestination(t *testing.T) {
	ids := make([]Id, MaxMultihopLength+1)
	for idIndex := range ids {
		ids[idIndex] = NewId()
	}
	multiHopId, err := NewMultiHopId(ids...)
	if err != nil {
		t.Fatal(err)
	}
	if multiHopId.Len() != len(ids) || multiHopId.Tail() != ids[len(ids)-1] {
		t.Fatalf("maximum path len=%d tail=%s", multiHopId.Len(), multiHopId.Tail())
	}
	encoded := multiHopId.Bytes()
	decoded, err := MultiHopIdFromBytes(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != multiHopId {
		t.Fatal("maximum path changed during byte serialization")
	}
	if _, err := NewMultiHopId(append(ids, NewId())...); err == nil {
		t.Fatal("path beyond the maximum was accepted")
	}
}

// Existing eight-id paths retain the same ordered byte representation after
// adding the ninth slot.
func TestMultiHopIdExistingMaximumSerializationIsUnchanged(t *testing.T) {
	ids := make([]Id, MaxMultihopLength)
	for idIndex := range ids {
		ids[idIndex] = NewId()
	}
	multiHopId, err := NewMultiHopId(ids...)
	if err != nil {
		t.Fatal(err)
	}
	encoded := multiHopId.Bytes()
	if len(encoded) != len(ids) {
		t.Fatalf("encoded id count=%d want=%d", len(encoded), len(ids))
	}
	for idIndex, idBytes := range encoded {
		if !bytes.Equal(idBytes, ids[idIndex].Bytes()) {
			t.Fatalf("encoded id %d changed", idIndex)
		}
	}
}

func TestByteCount(t *testing.T) {
	AssertEqual(t, ByteCountHumanReadable(ByteCount(0)), "0b")
	AssertEqual(t, ByteCountHumanReadable(ByteCount(5*1024*1024*1024*1024)), "5tib")

	count, err := ParseByteCount("2")
	AssertEqual(t, err, nil)
	AssertEqual(t, count, ByteCount(2))
	AssertEqual(t, ByteCountHumanReadable(count), "2b")

	count, err = ParseByteCount("5B")
	AssertEqual(t, err, nil)
	AssertEqual(t, count, ByteCount(5))
	AssertEqual(t, ByteCountHumanReadable(count), "5b")

	count, err = ParseByteCount("123KiB")
	AssertEqual(t, err, nil)
	AssertEqual(t, count, ByteCount(123*1024))
	AssertEqual(t, ByteCountHumanReadable(count), "123kib")

	count, err = ParseByteCount("5MiB")
	AssertEqual(t, err, nil)
	AssertEqual(t, count, ByteCount(5*1024*1024))
	AssertEqual(t, ByteCountHumanReadable(count), "5mib")

	count, err = ParseByteCount("1.7GiB")
	AssertEqual(t, err, nil)
	AssertEqual(t, count, ByteCount(17*1024*1024*1024)/ByteCount(10))
	AssertEqual(t, ByteCountHumanReadable(count), "1.7gib")

	count, err = ParseByteCount("13.1TiB")
	AssertEqual(t, err, nil)
	AssertEqual(t, count, ByteCount(131*1024*1024*1024*1024)/ByteCount(10))
	AssertEqual(t, ByteCountHumanReadable(count), "13.1tib")
}

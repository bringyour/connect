package connect

import (
	// "os"
	"encoding/json"
	"flag"
	"testing"

	"github.com/go-playground/assert/v2"
)

func init() {
	initGlog()
}

func initGlog() {
	flag.Set("logtostderr", "true")
	flag.Set("stderrthreshold", "INFO")
	flag.Set("v", "0")
}

func TestIdOrder(t *testing.T) {
	// ulids are ordered by create time
	// we use this property in the system, where ulids from the same source can be ordered

	a := NewId()
	for range 1024 * 1024 {
		b := NewId()
		assert.Equal(t, a.LessThan(b), true)
		assert.Equal(t, b.LessThan(a), false)
		assert.Equal(t, b.LessThan(b), false)
		assert.Equal(t, b == a, false)
		assert.Equal(t, b == b, true)
		a = b
	}
}

func TestIdJsonCodec(t *testing.T) {
	type Test struct {
		A Id  `json:"a,omitempty"`
		B *Id `json:"b,omitempty"`
	}

	test1 := &Test{}
	test1.A = NewId()
	b_ := NewId()
	test1.B = &b_

	test1Json, err := json.Marshal(test1)
	assert.Equal(t, err, nil)

	test2 := &Test{}
	err = json.Unmarshal(test1Json, test2)
	assert.Equal(t, err, nil)

	assert.Equal(t, test1.A, test2.A)
	assert.Equal(t, test1.B, test2.B)

	test3 := &Test{}
	test3.A = NewId()

	test3Json, err := json.Marshal(test3)
	assert.Equal(t, err, nil)

	test4 := &Test{}
	err = json.Unmarshal(test3Json, test4)
	assert.Equal(t, err, nil)

	assert.Equal(t, test3.A, test4.A)
	assert.Equal(t, test3.B, nil)
	assert.Equal(t, test3.B, test4.B)
}

func TestTransferPath(t *testing.T) {
	a := NewId()
	b := NewId()
	c := NewId()

	var path TransferPath
	var err error

	path, err = TransferPathFromBytes(nil, nil, nil)
	assert.Equal(t, err, nil)
	assert.Equal(t, path, TransferPath{})
	assert.Equal(t, path.IsStream(), false)
	assert.Equal(t, path.IsSourceMask(), true)
	assert.Equal(t, path.IsDestinationMask(), true)
	assert.Equal(t, path.IsControlSource(), true)
	assert.Equal(t, path.IsControlDestination(), true)

	path, err = TransferPathFromBytes(a.Bytes(), nil, nil)
	assert.Equal(t, err, nil)
	assert.Equal(t, path, TransferPath{SourceId: a})
	assert.Equal(t, path.IsStream(), false)
	assert.Equal(t, path.IsSourceMask(), true)
	assert.Equal(t, path.IsDestinationMask(), false)
	assert.Equal(t, path.IsControlSource(), false)
	assert.Equal(t, path.IsControlDestination(), false)

	path = SourceId(a)
	assert.Equal(t, path, TransferPath{SourceId: a})
	assert.Equal(t, path.IsStream(), false)
	assert.Equal(t, path.IsSourceMask(), true)
	assert.Equal(t, path.IsDestinationMask(), false)
	assert.Equal(t, path.IsControlSource(), false)
	assert.Equal(t, path.IsControlDestination(), false)

	path, err = TransferPathFromBytes(a.Bytes(), b.Bytes(), nil)
	assert.Equal(t, err, nil)
	assert.Equal(t, path, TransferPath{SourceId: a, DestinationId: b})
	assert.Equal(t, path.IsStream(), false)
	assert.Equal(t, path.IsSourceMask(), false)
	assert.Equal(t, path.IsDestinationMask(), false)
	assert.Equal(t, path.IsControlSource(), false)
	assert.Equal(t, path.IsControlDestination(), false)

	path, err = TransferPathFromBytes(nil, b.Bytes(), nil)
	assert.Equal(t, err, nil)
	assert.Equal(t, path, TransferPath{DestinationId: b})
	assert.Equal(t, path.IsStream(), false)
	assert.Equal(t, path.IsSourceMask(), false)
	assert.Equal(t, path.IsDestinationMask(), true)
	assert.Equal(t, path.IsControlSource(), false)
	assert.Equal(t, path.IsControlDestination(), false)

	path = DestinationId(b)
	assert.Equal(t, path, TransferPath{DestinationId: b})
	assert.Equal(t, path.IsStream(), false)
	assert.Equal(t, path.IsSourceMask(), false)
	assert.Equal(t, path.IsDestinationMask(), true)
	assert.Equal(t, path.IsControlSource(), false)
	assert.Equal(t, path.IsControlDestination(), false)

	path, err = TransferPathFromBytes(a.Bytes(), b.Bytes(), c.Bytes())
	assert.Equal(t, err, nil)
	assert.Equal(t, path, TransferPath{StreamId: c})
	assert.Equal(t, path.IsStream(), true)
	assert.Equal(t, path.IsSourceMask(), true)
	assert.Equal(t, path.IsDestinationMask(), true)
	assert.Equal(t, path.IsControlSource(), false)
	assert.Equal(t, path.IsControlDestination(), false)

	path, err = TransferPathFromBytes(nil, nil, c.Bytes())
	assert.Equal(t, err, nil)
	assert.Equal(t, path, TransferPath{StreamId: c})
	assert.Equal(t, path.IsStream(), true)
	assert.Equal(t, path.IsSourceMask(), true)
	assert.Equal(t, path.IsDestinationMask(), true)
	assert.Equal(t, path.IsControlSource(), false)
	assert.Equal(t, path.IsControlDestination(), false)

	path = StreamId(c)
	assert.Equal(t, path, TransferPath{StreamId: c})
	assert.Equal(t, path.IsStream(), true)
	assert.Equal(t, path.IsSourceMask(), true)
	assert.Equal(t, path.IsDestinationMask(), true)
	assert.Equal(t, path.IsControlSource(), false)
	assert.Equal(t, path.IsControlDestination(), false)

	assert.Equal(t, path.Reverse(), TransferPath{StreamId: c})

	path = NewTransferPath(a, b, Id{})
	assert.Equal(t, path.IsSourceMask(), false)
	assert.Equal(t, path.IsDestinationMask(), false)
	s := path.SourceMask()
	assert.Equal(t, s.IsSourceMask(), true)
	assert.Equal(t, s.IsDestinationMask(), false)
	d := path.DestinationMask()
	assert.Equal(t, d.IsSourceMask(), false)
	assert.Equal(t, d.IsDestinationMask(), true)

	assert.Equal(t, path.Reverse(), TransferPath{SourceId: b, DestinationId: a})
}

func TestMultiHopId(t *testing.T) {
	ids := []Id{
		NewId(),
		NewId(),
		NewId(),
	}

	m, err := NewMultiHopId(ids...)
	assert.Equal(t, err, nil)
	m2 := RequireMultiHopId(ids...)
	assert.Equal(t, m, m2)
	assert.Equal(t, m.Len(), 3)
	assert.Equal(t, len(m.Ids()), 3)
	assert.Equal(t, m.Ids()[0], ids[0])
	assert.Equal(t, m.Ids()[1], ids[1])
	assert.Equal(t, m.Ids()[2], ids[2])
	assert.Equal(t, len(m.Bytes()), 3)
	assert.Equal(t, m.Bytes()[0], ids[0].Bytes())
	assert.Equal(t, m.Bytes()[1], ids[1].Bytes())
	assert.Equal(t, m.Bytes()[2], ids[2].Bytes())

	assert.Equal(t, m.Tail(), ids[2])

	m3, tail := m.SplitTail()
	m4 := RequireMultiHopId(ids[0], ids[1])
	assert.Equal(t, tail, ids[2])
	assert.Equal(t, m3, m4)
}

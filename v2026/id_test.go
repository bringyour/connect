package connect

import (
	"encoding/json"
	"testing"
)

func TestIdOrder(t *testing.T) {
	// ulids are ordered by create time
	// we use this property in the system, where ulids from the same source can be ordered

	a := NewId()
	for range 1024 * 1024 {
		b := NewId()
		AssertEqual(t, a.LessThan(b), true)
		AssertEqual(t, b.LessThan(a), false)
		AssertEqual(t, b.LessThan(b), false)
		AssertEqual(t, b == a, false)
		AssertEqual(t, b == b, true)
		a = b
	}
}

var sinkId Id

func TestNewIdAllocs(t *testing.T) {
	// NewId is on the per-packet hot path (pack message ids); it must not allocate.
	allocs := testing.AllocsPerRun(10000, func() {
		sinkId = NewId()
	})
	if allocs != 0 {
		t.Fatalf("NewId allocates %v per call, want 0", allocs)
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
	AssertEqual(t, err, nil)

	test2 := &Test{}
	err = json.Unmarshal(test1Json, test2)
	AssertEqual(t, err, nil)

	AssertEqual(t, test1.A, test2.A)
	AssertEqual(t, test1.B, test2.B)

	test3 := &Test{}
	test3.A = NewId()

	test3Json, err := json.Marshal(test3)
	AssertEqual(t, err, nil)

	test4 := &Test{}
	err = json.Unmarshal(test3Json, test4)
	AssertEqual(t, err, nil)

	AssertEqual(t, test3.A, test4.A)
	AssertEqual(t, test3.B, nil)
	AssertEqual(t, test3.B, test4.B)
}

func TestIdJsonCodecIsIndependentOfContainerAddressability(t *testing.T) {
	id, err := ParseId("00112233-4455-6677-8899-aabbccddeeff")
	if err != nil {
		t.Fatal(err)
	}
	type record struct {
		Id Id `json:"id"`
	}

	valueJSON, err := json.Marshal(record{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	pointerJSON, err := json.Marshal(&record{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"id":"00112233-4455-6677-8899-aabbccddeeff"}`
	if string(valueJSON) != want || string(pointerJSON) != want {
		t.Fatalf("Id JSON depends on addressability: value=%s pointer=%s want=%s", valueJSON, pointerJSON, want)
	}

	var decoded record
	if err := json.Unmarshal(valueJSON, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Id != id {
		t.Fatalf("Id JSON round trip = %s, want %s", decoded.Id, id)
	}
}

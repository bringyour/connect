package connect

import (
	"encoding/binary"
	mathrand "math/rand"
	// "slices"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestCombine(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {

		type part struct {
			header [18]byte
			data   []byte
		}

		consecutive := func(n int) []byte {
			out := make([]byte, 4*n)
			for i := range n {
				binary.BigEndian.PutUint32(out[4*i:], uint32(i))
			}
			return out
		}

		// generate all the parts
		// keep one part for each header separate
		// add all the parts from group 1 with a addr
		// add all the parts from group 2 with b addr
		// verify close on all with b addr
		// verify consecutive data on close

		for range 32 {

			as := []*part{}
			bs := []*part{}
			keyLens := map[[17]byte]int{}

			for range 1024 {
				m := 2 + mathrand.Intn(16)
				splits := make([]int, m)
				n := 0
				for i := range m {
					s := 16 + mathrand.Intn(128)
					splits[i] = s
					n += s
				}
				data := consecutive(n)

				var header [18]byte
				mathrand.Read(header[0:16])
				header[16] = uint8(m)
				keyLens[[17]byte(header[:])] = n

				parts := make([]*part, 0, m)
				for i := range m {
					p := &part{
						header: header,
						data:   data[:splits[i]*4],
					}
					p.header[17] = uint8(i)
					parts = append(parts, p)
					data = data[splits[i]*4:]
				}
				mathrand.Shuffle(len(parts), func(i int, j int) {
					parts[i], parts[j] = parts[j], parts[i]
				})

				as = append(as, parts[:m-1]...)
				bs = append(bs, parts[m-1])
			}

			mathrand.Shuffle(len(as), func(i int, j int) {
				as[i], as[j] = as[j], as[i]
			})
			mathrand.Shuffle(len(bs), func(i int, j int) {
				bs[i], bs[j] = bs[j], bs[i]
			})

			addrA := &net.UDPAddr{
				IP:   testUnspecifiedIp(ipVersion),
				Port: 8080,
			}
			addrB := &net.UDPAddr{
				IP:   testIndexedIp(ipVersion, 1),
				Port: 8081,
			}

			settings := DefaultPacketTranslationSettings()
			settings.DnsMaxCombine = 2048
			settings.DnsMaxCombinePerAddress = 2048
			settings.DnsMaxCombineFragmentCount = 255
			settings.DnsMaxCombinedPacketByteCount = 2 * 1024 * 1024
			settings.DnsMaxCombineBytes = 512 * 1024 * 1024
			settings.DnsMaxCombineBytesPerAddress = 512 * 1024 * 1024
			cq := newCombineQueue(settings)

			for _, a := range as {
				out, limit, err := cq.Combine(addrA, a.header, MessagePoolCopy(a.data))
				AssertEqual(t, err, nil)
				AssertEqual(t, limit, false)
				AssertEqual(t, out, nil)
			}

			for _, b := range bs {
				out, limit, err := cq.Combine(addrB, b.header, MessagePoolCopy(b.data))
				AssertEqual(t, err, nil)
				AssertEqual(t, limit, false)

				AssertEqual(t, out.addr, addrB)
				m := keyLens[[17]byte(b.header[:])]
				AssertEqual(t, len(out.data), 4*m)
				AssertEqual(t, out.data, consecutive(m))
			}
		}

	})
}

func TestCombineTrim(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {

		settings := DefaultPacketTranslationSettings()
		settings.DnsMaxCombine = 1024 * 1024
		settings.DnsMaxCombinePerAddress = 1024
		settings.DnsMaxCombineFragmentCount = 255
		settings.DnsMaxCombinedPacketByteCount = 2 * 1024 * 1024
		settings.DnsMaxCombineBytes = 2 * 1024 * 1024 * 1024
		settings.DnsMaxCombineBytesPerAddress = 2 * 1024 * 1024 * 1024
		cq := newCombineQueue(settings)

		m := 128

		batchTimes := make([]time.Time, m)
		batchCounts := make([]int, m)

		for i := range m {
			n := 128 + mathrand.Intn(1024)
			batchCounts[i] = n

			for range n {
				var header [18]byte
				mathrand.Read(header[0:16])
				c := 8 + mathrand.Intn(128)
				header[16] = uint8(c)
				header[17] = uint8(mathrand.Intn(c))

				addr := &net.UDPAddr{
					IP:   testRandomIp(ipVersion),
					Port: 8080 + mathrand.Intn(1024),
				}
				data := make([]byte, 16+mathrand.Intn(1024))
				mathrand.Read(data)
				out, limit, err := cq.Combine(addr, header, data)
				AssertEqual(t, err, nil)
				AssertEqual(t, limit, false)
				AssertEqual(t, out, nil)
			}

			batchTimes[i] = time.Now()
			select {
			case <-time.After(time.Duration(8+mathrand.Intn(32)) * time.Millisecond):
			}
		}

		for i := range m {
			c := 0
			for _, n := range batchCounts[i:] {
				c += n
			}
			AssertEqual(t, cq.Len(), c)

			cq.RemoveOlder(batchTimes[i])
		}

		AssertEqual(t, cq.Len(), 0)

		// pump beyond the limits
		for i := range 4 * settings.DnsMaxCombine {
			var header [18]byte
			mathrand.Read(header[0:16])
			c := 8 + mathrand.Intn(128)
			header[16] = uint8(c)
			header[17] = uint8(mathrand.Intn(c))

			addr := &net.UDPAddr{
				IP:   testRandomIp(ipVersion),
				Port: 8080 + mathrand.Intn(1024),
			}
			data := make([]byte, 16+mathrand.Intn(1024))
			mathrand.Read(data)
			out, limit, err := cq.Combine(addr, header, data)
			AssertEqual(t, err, nil)
			AssertEqual(t, limit, settings.DnsMaxCombine <= i)
			AssertEqual(t, out, nil)
		}
		AssertEqual(t, int64(cq.Len()), settings.DnsMaxCombine)

	})
}

func TestCombineQueueBoundsFragmentFanoutAndRetainedBytes(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		settings := DefaultPacketTranslationSettings()
		settings.DnsMaxCombineFragmentCount = 2
		settings.DnsMaxCombineBytes = 4096
		settings.DnsMaxCombineBytesPerAddress = 4096
		settings.DnsMaxCombinedPacketByteCount = 128
		cq := newCombineQueue(settings)
		addr := &net.UDPAddr{IP: net.ParseIP(testLoopbackIp(ipVersion)), Port: 53}

		var tooMany [18]byte
		tooMany[16] = 3
		data := MessagePoolCopy(make([]byte, 8))
		_, limit, err := cq.Combine(addr, tooMany, data)
		if err != nil || !limit {
			MessagePoolReturn(data)
			t.Fatalf("fragment fanout = (limit=%t, err=%v), want bounded refusal", limit, err)
		}
		MessagePoolReturn(data)
		if cq.Len() != 0 || cq.RetainedByteCount() != 0 {
			t.Fatalf("refused fanout retained queue state: len=%d bytes=%d", cq.Len(), cq.RetainedByteCount())
		}

		var header [18]byte
		header[16] = 2
		first := MessagePoolCopy(make([]byte, 80))
		_, limit, err = cq.Combine(addr, header, first)
		if err != nil || limit {
			MessagePoolReturn(first)
			t.Fatalf("first bounded fragment = (limit=%t, err=%v)", limit, err)
		}
		retained := cq.RetainedByteCount()
		if retained <= 0 || settings.DnsMaxCombineBytes < retained {
			t.Fatalf("retained bytes = %d, cap %d", retained, settings.DnsMaxCombineBytes)
		}
		header[17] = 1
		second := MessagePoolCopy(make([]byte, 80))
		_, limit, err = cq.Combine(addr, header, second)
		if err != nil || !limit {
			MessagePoolReturn(second)
			t.Fatalf("oversized combined packet = (limit=%t, err=%v), want refusal", limit, err)
		}
		MessagePoolReturn(second)
		if cq.RetainedByteCount() != retained {
			t.Fatal("refused fragment changed retained-byte accounting")
		}

		// A second incomplete packet must be refused once the retained-capacity
		// ceiling is exactly the amount held by the first packet.
		settings.DnsMaxCombineBytes = retained
		settings.DnsMaxCombineBytesPerAddress = retained
		var anotherHeader [18]byte
		anotherHeader[0] = 1
		anotherHeader[16] = 2
		third := MessagePoolCopy(make([]byte, 8))
		_, limit, err = cq.Combine(addr, anotherHeader, third)
		if err != nil || !limit {
			MessagePoolReturn(third)
			t.Fatalf("retained-byte overflow = (limit=%t, err=%v), want refusal", limit, err)
		}
		MessagePoolReturn(third)
		if cq.Len() != 1 || cq.RetainedByteCount() != retained {
			t.Fatalf("retained-byte refusal changed queue: len=%d bytes=%d", cq.Len(), cq.RetainedByteCount())
		}
		cq.RemoveOlder(time.Now().Add(time.Hour))
		if cq.Len() != 0 || cq.RetainedByteCount() != 0 {
			t.Fatalf("expiry retained queue state: len=%d bytes=%d", cq.Len(), cq.RetainedByteCount())
		}
	})
}

func TestPump(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		settings := DefaultPacketTranslationSettings()
		pq := newPumpQueue(settings)

		addr := &net.UDPAddr{
			IP:   testUnspecifiedIp(ipVersion),
			Port: 8080,
		}

		added := []*pumpItem{}

		for i := range settings.DnsMaxPumpHostsPerAddress {
			var header [18]byte
			mathrand.Read(header[0:16])
			header[16] = uint8(1)
			header[17] = uint8(0)
			tld := []byte(fmt.Sprintf("foo%d.com", i))
			item := &pumpItem{
				addr:   addr,
				id:     uint16(i),
				header: header,
				tld:    tld,
			}
			limit := pq.Add(item)
			AssertEqual(t, limit, false)
			added = append(added, item)
		}

		for j := range 32 {
			otherAddr := &net.UDPAddr{
				IP:   testUnspecifiedIp(ipVersion),
				Port: 8081 + j,
			}
			lastItem := pq.RemoveLast(otherAddr)
			AssertEqual(t, lastItem, nil)
		}

		for i := range settings.DnsMaxPumpHostsPerAddress {
			var header [18]byte
			mathrand.Read(header[0:16])
			header[16] = uint8(1)
			header[17] = uint8(0)
			tld := []byte(fmt.Sprintf("foo%d.com", i))
			item := &pumpItem{
				addr:   addr,
				id:     uint16(i),
				header: header,
				tld:    tld,
			}
			limit := pq.Add(item)
			AssertEqual(t, limit, true)
			// not added
		}

		for i := len(added) - 1; 0 <= i; i -= 1 {
			item := added[i]
			lastItem := pq.RemoveLast(addr)
			AssertEqual(t, lastItem.addr, item.addr)
			AssertEqual(t, lastItem.id, item.id)
			AssertEqual(t, lastItem.header, item.header)
			AssertEqual(t, lastItem.tld, item.tld)
		}

		// add n random items
		// remove n and verify the items are in reverse order of added

		// add items more than the pump limit
		// test that the limit flag is set
		// remove all and verify up to the limit is returned in reverse order
	})
}

func TestPumpTrim(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {

		settings := DefaultPacketTranslationSettings()
		settings.DnsMaxPumpHosts = 1024 * 1024
		settings.DnsMaxPumpHostsPerAddress = 1024
		pq := newPumpQueue(settings)

		m := 128

		batchTimes := make([]time.Time, m)
		batchCounts := make([]int, m)

		for i := range m {
			n := 128 + mathrand.Intn(1024)
			batchCounts[i] = n

			for range n {
				var header [18]byte
				mathrand.Read(header[0:16])
				c := 1 + mathrand.Intn(128)
				header[16] = uint8(c)
				header[17] = uint8(mathrand.Intn(c))
				tld := []byte(fmt.Sprintf("foo%d.com", mathrand.Intn(1024)))

				addr := &net.UDPAddr{
					IP:   testRandomIp(ipVersion),
					Port: 8080 + mathrand.Intn(1024),
				}
				item := &pumpItem{
					addr:   addr,
					id:     uint16(mathrand.Intn(32 * 1024 * 1024)),
					header: header,
					tld:    tld,
				}
				limit := pq.Add(item)
				AssertEqual(t, limit, false)
			}

			batchTimes[i] = time.Now()
			select {
			case <-time.After(time.Duration(8+mathrand.Intn(32)) * time.Millisecond):
			}
		}

		for i := range m {
			c := 0
			for _, n := range batchCounts[i:] {
				c += n
			}
			AssertEqual(t, pq.Len(), c)

			pq.RemoveOlder(batchTimes[i])
		}

		AssertEqual(t, pq.Len(), 0)

		// pump beyond the limits
		for i := range 4 * settings.DnsMaxPumpHosts {
			var header [18]byte
			mathrand.Read(header[0:16])
			header[16] = uint8(1 + mathrand.Intn(128))
			header[17] = uint8(mathrand.Intn(128))
			tld := []byte(fmt.Sprintf("foo%d.com", mathrand.Intn(1024)))

			addr := &net.UDPAddr{
				IP:   testRandomIp(ipVersion),
				Port: 8080 + mathrand.Intn(1024),
			}
			item := &pumpItem{
				addr:   addr,
				id:     uint16(mathrand.Intn(32 * 1024 * 1024)),
				header: header,
				tld:    tld,
			}
			limit := pq.Add(item)
			// fmt.Printf("[%d]\n", i)
			AssertEqual(t, limit, settings.DnsMaxPumpHosts <= i)
		}
		AssertEqual(t, int64(pq.Len()), settings.DnsMaxPumpHosts)
	})
}

package connect

import (
	mathrand "math/rand"
	"testing"
)

func TestDnsRequestEncodeDecode(t *testing.T) {
	var encodeBuf [1024]byte
	var decodeBuf [1024]byte

	tld := []byte("a.dev.")
	tlds := [][]byte{
		[]byte("b.dev."),
		[]byte("c.bar."),
		[]byte("a.dev."),
	}

	for range 32 {
		rlen := 4*1024 + mathrand.Intn(32*1024)
		data := make([]byte, rlen)
		mathrand.Read(data)

		c := 0
		i := 0
		for i < len(data) {

			var header [18]byte
			mathrand.Read(header[0:16])
			header[16] = uint8(c)
			header[17] = 0

			n, encoded, err := encodeDnsRequest(uint16(i), header, data[i:], encodeBuf, tld)
			AssertEqual(t, err, nil)

			id, decodedHeader, decoded, decodedTld, err, otherData := decodeDnsRequest(encoded, decodeBuf, tlds)
			AssertEqual(t, err, nil)
			AssertEqual(t, id, uint16(i))
			AssertEqual(t, data[i:i+n], decoded)
			AssertEqual(t, header, decodedHeader)
			AssertEqual(t, decodedTld, tld)
			AssertEqual(t, otherData, false)

			i += n
			c += 1
		}

		AssertEqual(t, encodeDnsRequestCount(data, tld), c)
	}

	var header [18]byte
	mathrand.Read(header[0:16])
	header[16] = 0
	header[17] = 0

	_, encoded, err := encodeDnsRequest(uint16(0), header, make([]byte, 0), encodeBuf, tld)
	AssertEqual(t, err, nil)

	id, decodedHeader, decoded, decodedTld, err, otherData := decodeDnsRequest(encoded, decodeBuf, tlds)
	AssertEqual(t, err, nil)
	AssertEqual(t, id, uint16(0))
	AssertEqual(t, make([]byte, 0), decoded)
	AssertEqual(t, header, decodedHeader)
	AssertEqual(t, decodedTld, tld)
	AssertEqual(t, otherData, false)
}

func TestDnsResponseEncodeDecode(t *testing.T) {
	var encodeBuf [1024]byte
	var decodeBuf [1024]byte

	tld := []byte("a.dev.")
	tlds := [][]byte{
		[]byte("b.dev."),
		[]byte("c.bar."),
		[]byte("a.dev."),
	}

	for range 32 {
		rlen := 4*1024 + mathrand.Intn(32*1024)
		data := make([]byte, rlen)
		mathrand.Read(data)

		c := 0
		i := 0
		for i < len(data) {
			var header [18]byte
			mathrand.Read(header[0:16])
			header[16] = uint8(c)
			header[17] = 0

			n, encoded, err := encodeDnsResponse(uint16(i), header, header, data[i:], encodeBuf, tld)
			AssertEqual(t, err, nil)

			id, decodedPumpHeader, decodedHeader, decoded, err := decodeDnsResponse(encoded, decodeBuf, tlds)
			AssertEqual(t, err, nil)
			AssertEqual(t, id, uint16(i))
			AssertEqual(t, data[i:i+n], decoded)
			AssertEqual(t, header, decodedPumpHeader)
			AssertEqual(t, header, decodedHeader)

			i += n
			c += 1
		}

		AssertEqual(t, encodeDnsResponseCount(data, tld), c)
	}

	var header [18]byte
	mathrand.Read(header[0:16])
	header[16] = 0
	header[17] = 0

	_, encoded, err := encodeDnsResponse(uint16(0), header, header, make([]byte, 0), encodeBuf, tld)
	AssertEqual(t, err, nil)

	id, decodedPumpHeader, decodedHeader, decoded, err := decodeDnsResponse(encoded, decodeBuf, tlds)
	AssertEqual(t, err, nil)
	AssertEqual(t, id, uint16(0))
	AssertEqual(t, make([]byte, 0), decoded)
	AssertEqual(t, header, decodedPumpHeader)
	AssertEqual(t, header, decodedHeader)
}

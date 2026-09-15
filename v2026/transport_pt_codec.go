package connect

import (
	"encoding/base32"
	"encoding/base64"
	// "fmt"
	// mathrand "math/rand"
	"slices"

	// "golang.org/x/net/idna"
	"golang.org/x/net/dns/dnsmessage"
)

// returns the number of passes of encode needed for the packet and tld
func encodeDnsRequestCount(packet []byte, tld []byte) int {

	m := 157 - 18 - len(tld)

	c := len(packet) / m
	if len(packet)%m != 0 {
		c += 1
	}

	return c
}

// returns number of bytes read from packet, output buffer, error
func encodeDnsRequest(id uint16, header [18]byte, packet []byte, buf [1024]byte, tld []byte) (n int, out []byte, err error) {
	enc := base32.HexEncoding.WithPadding(base32.NoPadding)

	b := dnsmessage.NewBuilder(buf[:0], dnsmessage.Header{
		ID:            id,
		OpCode:        0,
		Authoritative: true,
	})
	b.EnableCompression()
	err = b.StartQuestions()
	if err != nil {
		return
	}
	name := dnsmessage.Name{}

	nameBuf := name.Data[:0]

	m := min(len(packet), 157-len(header)-len(tld))

	copy(buf[len(buf)-30:], header[:])
	j := min(30-len(header), m)
	copy(buf[len(buf)-30+len(header):], packet[0:j])
	nameBuf = enc.AppendEncode(nameBuf, buf[len(buf)-30:len(buf)-30+len(header)+j])
	nameBuf = append(nameBuf, []byte(".")...)

	for n = j; n < m; n = j {
		j = min(n+30, m)
		nameBuf = enc.AppendEncode(nameBuf, packet[n:j])
		nameBuf = append(nameBuf, []byte(".")...)
	}

	nameBuf = append(nameBuf, tld...)
	name.Length = uint8(len(nameBuf))

	err = b.Question(dnsmessage.Question{
		Name:  name,
		Type:  dnsmessage.TypeTXT,
		Class: dnsmessage.ClassINET,
	})
	if err != nil {
		return
	}

	// add the header as a second question, so that each request gets a response
	// the dns spec does not specify how two questions should be answered
	// we add it second so that it can be dropped, but it will look like an implementation/spec error
	// when the pump response comes to the second question instead of the first
	pumpName := dnsmessage.Name{}
	pumpNameBuf := pumpName.Data[:0]
	pumpNameBuf = enc.AppendEncode(pumpNameBuf, header[:])
	pumpNameBuf = append(pumpNameBuf, []byte(".")...)
	pumpNameBuf = append(pumpNameBuf, tld...)
	pumpName.Length = uint8(len(pumpNameBuf))
	err = b.Question(dnsmessage.Question{
		Name:  pumpName,
		Type:  dnsmessage.TypeTXT,
		Class: dnsmessage.ClassINET,
	})
	if err != nil {
		return
	}

	out, err = b.Finish()
	return
}

func decodeDnsRequest(packet []byte, buf [1024]byte, tlds [][]byte) (id uint16, header [18]byte, out []byte, tld []byte, err error, otherData bool) {
	enc := base32.HexEncoding.WithPadding(base32.NoPadding)

	p := &dnsmessage.Parser{}
	var h dnsmessage.Header
	h, err = p.Start(packet)
	if err != nil {
		return
	}
	id = h.ID

	out = buf[:]
	n := 0

	var qs []dnsmessage.Question
	qs, err = p.AllQuestions()
	if err != nil {
		return
	}
	for _, q := range qs {
		switch q.Type {
		case dnsmessage.TypeTXT:
			tld = func() []byte {
				for _, tld := range tlds {
					if len(tld) < int(q.Name.Length) &&
						slices.Equal(tld, q.Name.Data[int(q.Name.Length)-len(tld):int(q.Name.Length)]) &&
						q.Name.Data[int(q.Name.Length)-len(tld)-1] == '.' {
						return tld
					}
				}
				return nil
			}()
			if tld == nil {
				// a TXT question outside every encoding tld is a forwarder
				// query, not a translation query (A6). Without this it fell
				// through as an empty translation packet and was dropped, so
				// the forwarder never saw it.
				otherData = true
				continue
			}

			var j int
			for i := 0; i < int(q.Name.Length)-len(tld); i = j + 1 {
				for j = i; j < int(q.Name.Length)-len(tld); j += 1 {
					if q.Name.Data[j] == '.' {
						break
					}
				}

				var m int
				m, err = enc.Decode(out[n:], q.Name.Data[i:j])
				if err != nil {
					return
				}
				if 0 < n && i == 0 && m <= 18 {
					// header with no data, ignore this record
					break
				}
				n += m
			}
		default:
			otherData = true
		}

	}

	if 18 <= n {
		header = [18]byte(out[0:18])
		out = out[18:n]
	}

	return
}

// returns the number of passes of encode needed for the packet and tld
func encodeDnsResponseCount(packet []byte, tld []byte) int {

	m := 191 - 18 + 160 - len(tld)

	c := len(packet) / m
	if len(packet)%m != 0 {
		c += 1
	}

	return c
}

// https://www.iana.org/assignments/dns-parameters/dns-parameters.xhtml#dns-parameters-5

// returns number of bytes read from packet, output buffer, error
func encodeDnsResponse(id uint16, pumpHeader [18]byte, header [18]byte, packet []byte, buf [1024]byte, tld []byte) (n int, out []byte, err error) {
	enc := base32.StdEncoding.WithPadding(base64.NoPadding)
	rEnc := base64.StdEncoding.WithPadding(base64.NoPadding)

	b := dnsmessage.NewBuilder(buf[:0], dnsmessage.Header{
		ID:            id,
		Response:      true,
		Authoritative: true,
		RCode:         dnsmessage.RCodeSuccess,
	})
	b.EnableCompression()
	err = b.StartAnswers()
	if err != nil {
		return
	}

	name := dnsmessage.Name{}
	nameBuf := name.Data[:0]
	nameBuf = enc.AppendEncode(nameBuf, pumpHeader[:])
	nameBuf = append(nameBuf, []byte(".")...)
	nameBuf = append(nameBuf, tld...)
	name.Length = uint8(len(nameBuf))

	n = min(len(packet), 191-len(header)-len(tld))

	t := buf[512:512]
	t = rEnc.AppendEncode(t, header[:])
	t = rEnc.AppendEncode(t, packet[:n])

	err = b.TXTResource(
		dnsmessage.ResourceHeader{
			Name:  name,
			Type:  dnsmessage.TypeTXT,
			Class: dnsmessage.ClassINET,
			TTL:   0,
		},
		dnsmessage.TXTResource{
			TXT: []string{string(t)},
		},
	)
	if err != nil {
		return
	}

	if n < len(packet) {
		m := min(len(packet)-n, 160)

		t = buf[768:768]
		t = rEnc.AppendEncode(t, packet[n:n+m])

		err = b.TXTResource(
			dnsmessage.ResourceHeader{
				Name:  name,
				Type:  dnsmessage.TypeTXT,
				Class: dnsmessage.ClassINET,
				TTL:   0,
			},
			dnsmessage.TXTResource{
				TXT: []string{string(t)},
			},
		)
		if err != nil {
			return
		}

		n += m
	}

	out, err = b.Finish()
	// fmt.Printf("F (%d) %d = %s\n", n, len(out), string(out))
	return
}

func decodeDnsResponse(packet []byte, buf [1024]byte, tlds [][]byte) (id uint16, pumpHeader [18]byte, header [18]byte, out []byte, err error) {
	enc := base32.StdEncoding.WithPadding(base32.NoPadding)
	rEnc := base64.StdEncoding.WithPadding(base64.NoPadding)

	p := &dnsmessage.Parser{}
	var h dnsmessage.Header
	h, err = p.Start(packet)
	if err != nil {
		// fmt.Printf("ERROR 0\n")
		return
	}
	id = h.ID

	p.SkipAllQuestions()

	var as []dnsmessage.Resource
	as, err = p.AllAnswers()
	if err != nil {
		// fmt.Printf("ERROR 1\n")
		return
	}

	out = buf[:]
	n := 0

	for _, a := range as {

		tld := func() []byte {
			for _, tld := range tlds {
				if len(tld) < int(a.Header.Name.Length) &&
					slices.Equal(tld, a.Header.Name.Data[int(a.Header.Name.Length)-len(tld):int(a.Header.Name.Length)]) &&
					a.Header.Name.Data[int(a.Header.Name.Length)-len(tld)-1] == '.' {
					return tld
				}
			}
			return nil
		}()
		if tld == nil {
			continue
		}

		var j int
		for j = 0; j < int(a.Header.Name.Length)-len(tld); j += 1 {
			if a.Header.Name.Data[j] == '.' {
				break
			}
		}

		_, err = enc.Decode(pumpHeader[0:], a.Header.Name.Data[0:j])
		if err != nil {
			return
		}

		switch a.Header.Type {
		case dnsmessage.TypeTXT:
			r := a.Body.(*dnsmessage.TXTResource)

			for _, txt := range r.TXT {
				var m int
				m, err = rEnc.Decode(out[n:], []byte(txt))
				if err != nil {
					// fmt.Printf("ERROR 3\n")
					return
				}
				n += m
			}
			// else ignore
		}

	}

	if 18 <= n {
		header = [18]byte(out[0:18])
		out = out[18:n]
	}

	return
}

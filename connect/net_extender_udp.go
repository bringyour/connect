package connect


import (


	"dns"
)




/*
 to make this work:

1. the extender needs to be root dns of a zone
   so the hostnames will be x.ZONE, where a single extender IP is mapped as the DNS for zone
2. client -> server uses request for A/AAAA records
3. server -> client uses TXT records
   the way it works is client sends W parallel requests for <pingframe>.ZONE TXT
   if the server has data to send, it replies to one of these requests with the data
   the server hangs onto response for a small timeout before sending
   client sends new ping requests in the window when they time out


*/




// first test: throughput measurement of just sending dns to and from each other
// each side continuously sends how much data received, plus random data
// 


type DnsPayloadFramer interface {
	PayloadSize(frameLen int) int
	Frame(payload []byte) []byte
}
/*
Server names and ips are unique to each request and always random
Initial request contains the session init: public key
Response payload contains:  session id, 
Next request payloads are:  (session id + ENCRYPT(transfer stats, payload)) XOR (number of 0 bits mod 255)
(8-bit length big endian)
Next response payloads are:  ECRYPT(transfer stats, payload)
*/


func EncodeDnsRequestMsg(framer DnsPayoutFramer, payload []byte) (msg *Msg, n int, returnErr error) {

	// random hostFqn
	// Question SERVER NAME, record random A/AAAA (not used as data)

	b := 128

	m := min(framer.PayloadSize(b), len(payload))
	if m == 0 {
		panic(fmt.Errorf("Encoding assumption invalid."))
	}

	frame := framer.Frame(payload[:n])
	n = m

	fqn, m, err := EncodeFqn(frame, ".com", 255)
	if err != nil {
		returnErr = err
		return
	}
	if m != n {
		panic(fmt.Errorf("Encoding assumption failed."))
	}
	// aleays use A, we won't use the record values to encode
	// var recordType string
	// switch mathrand.Intn(2) {
	// case 0:
	// 	recordType = "A"
	// case 1:
	// 	recordType = "AAAA"
	// }
	msg = &dns.Msg{}
	msg.Question = append(msg.Question, dns.Question{
		Name: fqn,
		Qtype: A,
	})
	// msgBytes, err := msq.Pack()
	// if err != nil {
	// 	panic(fmt.Errorf("Message length assumption failed."))
	// }
	// return
	return
}

func EncodeDnsResponseMsg(framer DnsPayoutFramer, payload []byte) (msg *Msg, n int, returnErr error) {
	// random hostFqn

	// ANSWER SERVER NAME, encode A/AAAA value
	// EXTRA SERVER NAME, encode A/AAAA value

	// separate all answers into independent frames
	// in this way, one section can be dropped and the stream can still make process



	// FIXME encode independent frames for answer and extra
	// FIXME this is not as efficient, but it makes the stream resilient to extras being dropped

	b := 128
	// standard headers are 32 bytes
	msgMaxN := 510 - 32

	// adaptive ratio, (msg entry len)/(frame len)
	encodingRatio := float32(1.6)


	msg = &dns.Msg{}
	n = 0

	msgN := 0


	for i := 0; msgN + b * encodingRatio < msgMaxN; i += 1 {


		m := min(framer.PayloadSize(b), len(payload) - n)
		if m == 0 {
			panic(fmt.Errorf("Encoding assumption invalid."))
		}
		frame := framer.Frame(payload[n:n+m])
		



		var recordType string
		recordType = "A"


		ip = RANDOM()

		fqn, m, err := EncodeFqn(frame, ".com", 255)
		if err != nil {
			returnErr = err
			return
		}
		if m != a {
			panic(fmt.Errorf("Encoding assumption failed."))
		}


		// TODO the entry header and ip value is 16 bytes
		msgEntryN = len(fqn) + 16
		if msgMaxN <= msgN + msgEntryN {
			// estimates were off, not enough room
			break
		}
		n += m

		msgN += msgEntryN
		encodingRatio = float32(msgEntryN) / float32(len(frame))

		rr := &A{
			RR_Header: &RR_Header{
				Name: fqn,
				Class: A,
			}
			A: ip,
		}

		if i == 0 {
			// add the first frame to the answers
			msg.Answers = append(msg.Answers, rr)
		} else {
			msg.Extras = append(msg.Extras, rr)
		}

	}
	
	return msg
}



func DecodeDnsRequestMsg(msg Msg) [][]byte {
	// each question is an independent message
	
	fqn := ""
	frame := DecodeFqn(fqn)
	return [][]byte{
		frame,
	}
}

func DecodeDnsResponseMsg(msg Msg) [][]byte {
	// answers and extras are independent messages


	// GET answer
	// GET extra

	frames := [][]byte{}
	for _, rr := range msg.Answers {
		frames = append(frames, DecodeFqn(fqn))
	}
	for _, rr := range msg.Extras {
		frames = append(frames, DecodeFqn(fqn))
	}

	return frames
}



// within the context of domain names, output a json of letter, number, or dash to frequency of usage, normalized so that the most frequent is 1.0
fqnRunes := map[rune]float {
  "e": 1.0,
  "a": 0.82,
  "o": 0.75,
  "i": 0.69,
  "n": 0.67,
  "r": 0.62,
  "t": 0.59,
  "s": 0.57,
  "l": 0.41,
  "c": 0.28,
  "u": 0.27,
  "d": 0.25,
  "p": 0.23,
  "m": 0.22,
  "h": 0.21,
  "g": 0.20,
  "b": 0.15,
  "f": 0.14,
  "y": 0.13,
  "w": 0.12,
  "k": 0.08,
  "v": 0.07,
  "x": 0.02,
  "z": 0.01,
  "j": 0.01,
  "q": 0.01,
  "0": 0.15,
  "1": 0.14,
  "2": 0.12,
  "3": 0.10,
  "4": 0.09,
  "5": 0.08,
  "6": 0.07,
  "7": 0.07,
  "8": 0.06,
  "9": 0.06,
  "-": 0.04,
}
// equivalency sets have the same value regardless of rune, and each rune is chosen based on its weight in the set
fqnRuneEquivalencies := map[rune]int {
	"-": 0,
	"x": 0,
	"z": 0,
	"j": 1,
	"q": 1,
	"6": 2,
	"7": 2,
	"8": 3,
	"9": 3,
}
// total fqn rune size with equivalencies is 32 (5-bit)
// x, z, dash are excluded (equivalency 0)
// use a 5-bit encoding with zero padding


encodeRunes := []rune{
	'e',
	'a',
	'o',
	'i',
	'n',
	'r',
	't',
	's',
	'l',
	'c',
	'u',
	'd',
	'p',
	'm',
	'h',
	'g',
	'b',
	'f',
	'y',
	'w',
	'k',
	'v',
	'j',
	'0',
	'1',
	'2',
	'3',
	'4',
	'5',
	'6',
	'8',
}
decodeRunes := func()(map[rune]int){
	m := map[rune]int{}
	for i, rune := range encodeRunes {
		m[rune] = i
	}
	return m
}()
excludedRunes := map[rune]float {
	'-': 0.04,
	'x': 0.02,
	'z': 0.01,
}



// sprinkles excluded in based on frequency
// if starts with digit, use an excluded letter to start
// never start with dash
// fqHost must be name.tld. if it is x.name.tld or more subdomains, only name.tld is used
//    if fqHost is just tld, the generated name is split to have a host randomly between 4 and 60 chars
// use single level sub domain
func EncodeFqn(payload []bytes, fqHost string, maxLen int) (fqn string, n int, returnErr error) {

	fqHostParts := strings.Split(fqHost, ".")
	if len(fqHostParts) == 0 {
		returnErr = fmt.Errorf("Invalid fq host: %s", fqHost)
		return
	} else if 2 < len(fqHostParts) {
		// keep only the top 2
		fqHost = strings.Join(".", fqHostParts[len(fqHostParts) - 2:]...)
	}
	

	// 0
	// (a >> 3)
	// 1
	// ((a << 1) >> 3)
	// 2
	// ((a << 2) >> 3)
	// 3
	// ((a << 3) >> 3)
	// 4
	// ((a << 4) >> 3) | (b >> 7)
	// 5
	// ((a << 5) >> 3) | (b >> 6)
	// 6
	// ((a << 6) >> 3) | (b >> 5)
	// 7
	// ((a << 7) >> 3) | (b >> 4)

	unpack := func(a byte, b byte, biti int)(byte) {
		switch biti {
		case 0:
			return a >> 3
		case 1:
			return (a << 1) >> 3
		case 2:
			return (a << 2) >> 3
		case 3:
			return (a << 3) >> 3
		case 4:
			return ((a << 4) >> 3) | (b >> 7)
		case 5:
			return ((a << 5) >> 3) | (b >> 6)
		case 6:
			return ((a << 6) >> 3) | (b >> 5)
		case 7:
			return ((a << 7) >> 3) | (b >> 4)
		}
	}

	biti := 0
	i := 0
	for ; i < len(payload) && len(runes) < maxLen; i += 1 {
		var v byte
		if i + 1 < len(payload) {
			v = unpack(payload[i], payload[i + 1], biti)
		} else {
			v = unpack(payload[i], 0, biti)
		}
		biti = (biti + 5) % 8
		runes = append(runes, encodeRunes[v])

		// add noise
		for rune, p := range excludedRunes {
			if len(runes) < maxLen && mathrand.Float32n() < p {
				runes = append(runes, rune)
			}
		}
	}

	fqn = strings.Join(".", string(runes), fqHost)
	n = i
	return
}

func DecodeFqn(fqn string) (payload []byte, returnErr error) {

	// 0
	// a |= (v << 3)
	// 1
	// a |= (v << 2)
	// 2
	// a |= (v << 1)
	// 3
	// a |= v
	// 4
	// a |= (v >> 1)
	// b |= (v << 7)
	// 5
	// a |= (v >> 2)
	// b |= (v << 6)
	// 6
	// a |= (v >> 3)
	// b |= (v << 5)
	// 7
	// a |= (v >> 4)
	// b |= (v << 4)
	
	
	pack := func(a byte, b byte, biti int, v byte)(byte, byte) {
		switch biti {
		case 0:
			a |= v << 3
		case 1:
			a |= v << 2
		case 2:
			a |= v << 1
		case 3:
			a |= v
		case 4:
			a |= v >> 1
			b |= v << 7
		case 5:
			a |= v >> 2
			b |= v << 6
		case 6:
			a |= v >> 3
			b |= v << 5
		case 7:
			a |= v >> 4
			b |= v << 4
		}
		return a, b
	}

	payload = []byte{}
	a := byte(0)
	b := byte(0)
	biti := 0
	for rune := range fqn {
		rune = unicode.ToLower(rune)
		if _, ok := excludedRunes[rune]; ok {
			continue
		}
		v, ok := decodeRunes[rune]
		if !ok {
			// unknown rune
			returnErr = fmt.Errorf("Unknown rune: %s", rune)
			return
		}
		a, b = pack(a, b, biti, v)
		biti += 5
		if 8 <= biti {
			payload = append(payload, a)
			a = b
			b = 0
			biti -= 8
		}
	}
	if b != 0 {
		panic(fmt.Errorf("Unexpected trailing value."))
	}

	return

}







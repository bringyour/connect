package data

import (
	"encoding/binary"
	"fmt"

	"golang.org/x/crypto/cryptobyte"
	"src.agwa.name/tlshacks"
)

type tlsHeader struct {
	contentType   byte
	tlsVersion    uint16
	contentLength uint16
}

func parseTlsHeader(b []byte) *tlsHeader {
	return &tlsHeader{
		contentType:   b[0],
		tlsVersion:    binary.BigEndian.Uint16(b[1:3]),
		contentLength: binary.BigEndian.Uint16(b[3:5]),
	}
}

// if serverName == "" then there was no server name in the ClientHello
// getting ("", nil) means ignore
func parseTls(tlsData []byte) (serverName string, err error) {
	// data too short (not TLS)
	if len(tlsData) < 5 {
		return "", nil
	}
	tlsHeader := parseTlsHeader(tlsData)
	leftData := tlsData[5:]
	// data shorter than expected (ignore)
	if len(leftData) < int(tlsHeader.contentLength) {
		return "", nil
	}
	if tlsHeader.contentType != 22 {
		// not handshake (ignore)
		return "", nil
	}
	handshakeBytes := leftData[:tlsHeader.contentLength]
	clientHello := UnmarshalClientHello(handshakeBytes)

	if clientHello == nil {
		return "", fmt.Errorf("failed to parse ClientHello")
	}
	if clientHello.Info.ServerName == nil {
		// no server name (ignore)
		return "", nil
	}

	// we have a server name
	return *clientHello.Info.ServerName, nil
}

func UnmarshalClientHello(handshakeBytes []byte) *tlshacks.ClientHelloInfo {
	info := &tlshacks.ClientHelloInfo{Raw: handshakeBytes}
	handshakeMessage := cryptobyte.String(handshakeBytes)

	var messageType uint8
	if !handshakeMessage.ReadUint8(&messageType) || messageType != 1 {
		return nil
	}

	var clientHello cryptobyte.String
	if !handshakeMessage.ReadUint24LengthPrefixed(&clientHello) || !handshakeMessage.Empty() {
		return nil
	}

	if !clientHello.ReadUint16((*uint16)(&info.Version)) {
		return nil
	}

	if !clientHello.ReadBytes(&info.Random, 32) {
		return nil
	}

	if !clientHello.ReadUint8LengthPrefixed((*cryptobyte.String)(&info.SessionID)) {
		return nil
	}

	var cipherSuites cryptobyte.String
	if !clientHello.ReadUint16LengthPrefixed(&cipherSuites) {
		return nil
	}
	info.CipherSuites = []tlshacks.CipherSuite{}
	for !cipherSuites.Empty() {
		var suite uint16
		if !cipherSuites.ReadUint16(&suite) {
			return nil
		}
		info.CipherSuites = append(info.CipherSuites, tlshacks.MakeCipherSuite(suite))
	}

	var compressionMethods cryptobyte.String
	if !clientHello.ReadUint8LengthPrefixed(&compressionMethods) {
		return nil
	}
	info.CompressionMethods = []tlshacks.CompressionMethod{}
	for !compressionMethods.Empty() {
		var method uint8
		if !compressionMethods.ReadUint8(&method) {
			return nil
		}
		info.CompressionMethods = append(info.CompressionMethods, tlshacks.CompressionMethod(method))
	}

	info.Extensions = []tlshacks.Extension{}

	if clientHello.Empty() {
		return info
	}

	var extensions cryptobyte.String
	if !clientHello.ReadUint16LengthPrefixed(&extensions) {
		return nil
	}

	extensionParsers := map[uint16]func([]byte) tlshacks.ExtensionData{
		0:  tlshacks.ParseServerNameData,
		10: tlshacks.ParseSupportedGroupsData,
		11: tlshacks.ParseECPointFormatsData,
		16: tlshacks.ParseALPNData,
		18: tlshacks.ParseEmptyExtensionData,
		22: tlshacks.ParseEmptyExtensionData,
		23: tlshacks.ParseEmptyExtensionData,
		49: tlshacks.ParseEmptyExtensionData,
	}

	for !extensions.Empty() {
		var extType uint16
		var extData cryptobyte.String

		if !extensions.ReadUint16(&extType) || !extensions.ReadUint16LengthPrefixed(&extData) {
			return nil
		}

		parseData := extensionParsers[extType]
		if parseData == nil {
			parseData = tlshacks.ParseUnknownExtensionData
		}
		data := parseData(extData)

		info.Extensions = append(info.Extensions, tlshacks.Extension{
			Type:    extType,
			Name:    tlshacks.Extensions[extType].Name,
			Grease:  tlshacks.Extensions[extType].Grease,
			Private: tlshacks.Extensions[extType].Private,
			Data:    data,
		})

		switch extType {
		case 0:
			info.Info.ServerName = &data.(*tlshacks.ServerNameData).HostName
		case 16:
			info.Info.Protocols = data.(*tlshacks.ALPNData).Protocols
		case 18:
			info.Info.SCTs = true
		}

	}

	if !clientHello.Empty() {
		return nil
	}

	info.Info.JA3String = tlshacks.JA3String(info)
	info.Info.JA3Fingerprint = tlshacks.JA3Fingerprint(info.Info.JA3String)

	return info
}

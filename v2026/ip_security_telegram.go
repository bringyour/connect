package connect

// Telegram call reflector exception.
//
// Telegram publishes its current reflector endpoints at
// https://core.telegram.org/getReflectorList. Calls use the privileged port
// range 596-599, which the general CFAA policy correctly rejects for arbitrary
// hosts. The official iOS client also carries one protocol-v12 TCP fallback at
// 91.108.9.38:595. This file keeps both exceptions explicit and narrow: exact
// IPv4 endpoints, exact ports and transports. The caller performs the
// threat-intelligence blocklist check before consulting this exception.
//
// Snapshot verified 2026-09-01: 154 IPv4 addresses and 616 address/port pairs.
// The upstream list currently publishes no IPv6 reflectors.

import (
	"encoding/binary"
	"net"
)

type telegramCallIpv4Range struct {
	lo uint32
	hi uint32
}

// These sorted, non-overlapping ranges are a compact representation of the
// exact addresses returned by getReflectorList, not Telegram's broader AS or
// application-service prefixes.
var telegramCallReflectorIpv4Ranges = [...]telegramCallIpv4Range{
	{0x5b6c0901, 0x5b6c090a}, // 91.108.9.1-10
	{0x5b6c0911, 0x5b6c0911}, // 91.108.9.17
	{0x5b6c0913, 0x5b6c091a}, // 91.108.9.19-26
	{0x5b6c0921, 0x5b6c0925}, // 91.108.9.33-37
	{0x5b6c0927, 0x5b6c092a}, // 91.108.9.39-42
	{0x5b6c0931, 0x5b6c093a}, // 91.108.9.49-58
	{0x5b6c0942, 0x5b6c094a}, // 91.108.9.66-74
	{0x5b6c0951, 0x5b6c095a}, // 91.108.9.81-90
	{0x5b6c0961, 0x5b6c096a}, // 91.108.9.97-106
	{0x5b6c0971, 0x5b6c0979}, // 91.108.9.113-121
	{0x5b6c0d02, 0x5b6c0d0a}, // 91.108.13.2-10
	{0x5b6c0d11, 0x5b6c0d1a}, // 91.108.13.17-26
	{0x5b6c0d21, 0x5b6c0d2a}, // 91.108.13.33-42
	{0x5b6c0d31, 0x5b6c0d39}, // 91.108.13.49-57
	{0x5b6c1101, 0x5b6c110a}, // 91.108.17.1-10
	{0x5b6c1111, 0x5b6c111a}, // 91.108.17.17-26
	{0x5b6c1121, 0x5b6c112a}, // 91.108.17.33-42
	{0x5b6c1131, 0x5b6c113a}, // 91.108.17.49-58
}

// Telegram-iOS OngoingCallContext adds this endpoint only for call protocol
// version 12.0.0. Keeping it separate from the generated reflector-list
// snapshot documents why neither adjacent hosts nor UDP/595 are allowed.
const telegramCallV12TcpFallbackIpv4 uint32 = 0x5b6c0926 // 91.108.9.38

func isTelegramCallReflector(ip net.IP, port int, protocol IpProtocol, version int) bool {
	if version != 4 {
		return false
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return false
	}
	v := binary.BigEndian.Uint32(ip4)
	if v == telegramCallV12TcpFallbackIpv4 {
		return port == 595 && protocol == IpProtocolTcp
	}
	if port < 596 || 599 < port ||
		(protocol != IpProtocolTcp && protocol != IpProtocolUdp) {
		return false
	}

	lo, hi := 0, len(telegramCallReflectorIpv4Ranges)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if telegramCallReflectorIpv4Ranges[mid].hi < v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(telegramCallReflectorIpv4Ranges) &&
		telegramCallReflectorIpv4Ranges[lo].lo <= v
}

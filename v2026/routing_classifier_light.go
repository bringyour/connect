package connect

import (
	"net/netip"
	"strings"
)

// ServerNameResolver resolves a destination IP to the hostname the mux has
// already observed for it (a DNS answer or a sniffed TLS SNI), e.g.
// UpgradeMux's IP->hostname reverse index. It must be a pure table lookup --
// no I/O, no DNS query, safe for concurrent calls -- because it is called
// from LightClassifier.Classify on the placement path at new-flow frequency.
// A nil ServerNameResolver is valid: the server-name tier is simply skipped.
type ServerNameResolver func(ip netip.Addr) (string, bool)

// appMatchConfidence, serverNameMatchConfidence and portMatchConfidence are
// the FlowClass.Confidence values (0-100) reported per precedence tier. Higher
// tiers are more specific and get higher confidence. ClassUnknown results
// carry no confidence (the zero value), matching classifyOrUnknown's nil-
// classifier return.
const (
	appMatchConfidence        uint8 = 90
	serverNameMatchConfidence uint8 = 75
	portMatchConfidence       uint8 = 50
)

// LightClassifier is the pure-Go "light tier" FlowClassifier (spec §2):
// static table lookups only, no I/O, safe for concurrent use, and safe to run
// inline on the placement path. Precedence, highest first:
//
//	manual override (not implemented here -- a later phase)
//	> app default   (the flow's owning exe, from the FlowOwner seam)
//	> server name   (resolved via names, the IP->hostname reverse index)
//	> port
//
// A flow that matches none of these returns ClassUnknown: callers must never
// guess a class from partial information -- ClassUnknown is the signal that
// tells scoredPlacementReorder to fall through to the legacy (unscored)
// order.
type LightClassifier struct {
	names ServerNameResolver
}

// NewLightClassifier builds a LightClassifier that resolves hostnames for the
// server-name tier through names. names may be nil, in which case the
// server-name tier is skipped and classification falls through to the app
// and port tiers only.
func NewLightClassifier(names ServerNameResolver) *LightClassifier {
	return &LightClassifier{names: names}
}

var _ FlowClassifier = (*LightClassifier)(nil)

// Classify implements FlowClassifier. It never performs I/O and never
// blocks: every tier is a map/slice lookup over a static table.
func (self *LightClassifier) Classify(ipPath *IpPath, appId string) FlowClass {
	// appId is the highest-precedence tier and never dereferences ipPath, so
	// it must run even when ipPath is nil. The nil guard sits in front of
	// the tiers below that actually read ipPath's fields, not in front of
	// this one.
	if class, ok := classifyByApp(appId); ok {
		return FlowClass{Class: class, AppId: appId, Confidence: appMatchConfidence}
	}

	if ipPath == nil {
		return FlowClass{Class: ClassUnknown, AppId: appId}
	}

	if self.names != nil {
		if addr, ok := netIPAddr(ipPath.DestinationIp); ok {
			if name, ok := self.names(addr); ok {
				if class, ok := classifyByServerName(name); ok {
					return FlowClass{Class: class, AppId: appId, Confidence: serverNameMatchConfidence}
				}
			}
		}
	}

	if class, ok := classifyByPort(ipPath.DestinationPort); ok {
		return FlowClass{Class: class, AppId: appId, Confidence: portMatchConfidence}
	}

	return FlowClass{Class: ClassUnknown, AppId: appId}
}

// classifyByApp looks up the owning executable in appClassTable. appId may
// carry a full path (platform-dependent FlowOwner shape), so matching is
// case-insensitive on the base file name only.
func classifyByApp(appId string) (TrafficClass, bool) {
	if appId == "" {
		return ClassUnknown, false
	}
	base := appId
	if i := strings.LastIndexAny(base, `/\`); i >= 0 {
		base = base[i+1:]
	}
	class, ok := appClassTable[strings.ToLower(base)]
	return class, ok
}

// classifyByServerName matches name against serverNameSuffixTable: an exact
// match or a match on a "."+suffix boundary, so "cdn.netflix.com" matches the
// "netflix.com" entry but "notnetflix.com" does not.
func classifyByServerName(name string) (TrafficClass, bool) {
	name = strings.ToLower(strings.TrimSuffix(name, "."))
	if name == "" {
		return ClassUnknown, false
	}
	for _, entry := range serverNameSuffixTable {
		if name == entry.suffix || strings.HasSuffix(name, "."+entry.suffix) {
			return entry.class, true
		}
	}
	return ClassUnknown, false
}

// classifyByPort looks up the destination port in portClassTable. Keyed by
// port alone (not protocol): the ports below carry the same meaning over TCP
// or UDP (e.g. 443 is HTTPS whether QUIC or TLS-over-TCP).
func classifyByPort(port int) (TrafficClass, bool) {
	class, ok := portClassTable[port]
	return class, ok
}

// appClassTable maps a known owning executable (base name, lowercased) to its
// traffic class. A starter set; extend as real telemetry identifies more.
var appClassTable = map[string]TrafficClass{
	// bulk / download-heavy apps
	"steam.exe":             ClassBulk,
	"steamwebhelper.exe":    ClassBulk,
	"steamservice.exe":      ClassBulk,
	"epicgameslauncher.exe": ClassBulk,
	"battle.net.exe":        ClassBulk,
	"bittorrent.exe":        ClassBulk,
	"qbittorrent.exe":       ClassBulk,
	"utorrent.exe":          ClassBulk,
	"transmission.exe":      ClassBulk,

	// latency-sensitive: realtime voice/video/gaming apps
	"discord.exe": ClassLatency,
	"zoom.exe":    ClassLatency,
	"teams.exe":   ClassLatency,
	"skype.exe":   ClassLatency,

	// streaming apps
	"spotify.exe": ClassStreaming,
	"vlc.exe":     ClassStreaming,

	// background: sync / update agents
	"onedrive.exe":               ClassBackground,
	"dropbox.exe":                ClassBackground,
	"backgroundtransferhost.exe": ClassBackground,
}

// serverNameSuffixEntry is one entry of serverNameSuffixTable.
type serverNameSuffixEntry struct {
	suffix string
	class  TrafficClass
}

// serverNameSuffixTable maps a hostname suffix to its traffic class,
// evaluated in order (first match wins). A starter set; extend as real
// telemetry identifies more.
var serverNameSuffixTable = []serverNameSuffixEntry{
	// streaming: video/audio streaming services
	{"netflix.com", ClassStreaming},
	{"nflxvideo.net", ClassStreaming},
	{"nflximg.net", ClassStreaming},
	{"youtube.com", ClassStreaming},
	{"googlevideo.com", ClassStreaming},
	{"ytimg.com", ClassStreaming},
	{"twitch.tv", ClassStreaming},
	{"ttvnw.net", ClassStreaming},
	{"hulu.com", ClassStreaming},
	{"disneyplus.com", ClassStreaming},
	{"disney-plus.net", ClassStreaming},
	{"spotify.com", ClassStreaming},
	{"scdn.co", ClassStreaming},
	{"primevideo.com", ClassStreaming},
	{"bloomberg.com", ClassStreaming},
	{"bwbx.io", ClassStreaming},

	// bulk: large-transfer / distribution
	{"steampowered.com", ClassBulk},
	{"steamcontent.com", ClassBulk},
	{"windowsupdate.com", ClassBulk},
	{"delivery.mp.microsoft.com", ClassBulk},

	// background: system/telemetry chatter
	{"ntp.org", ClassBackground},
}

// portClassTable maps a destination port to its traffic class. A starter
// set; extend as real telemetry identifies more.
var portClassTable = map[int]TrafficClass{
	// latency-sensitive: realtime signaling / NAT traversal / VoIP
	3478:  ClassLatency, // STUN/TURN
	3479:  ClassLatency, // STUN/TURN (alt)
	19302: ClassLatency, // Google STUN
	5060:  ClassLatency, // SIP
	5061:  ClassLatency, // SIP/TLS

	// bulk: large-transfer / file-sharing protocols
	51413: ClassBulk, // BitTorrent (common default)
	6881:  ClassBulk, // BitTorrent range
	6882:  ClassBulk,
	6883:  ClassBulk,
	6884:  ClassBulk,
	6885:  ClassBulk,
	6886:  ClassBulk,
	6887:  ClassBulk,
	6888:  ClassBulk,
	6889:  ClassBulk,
	989:   ClassBulk, // FTPS data
	990:   ClassBulk, // FTPS control

	// browsing: default web ports, absent a more specific signal
	80:   ClassBrowsing,
	443:  ClassBrowsing,
	8080: ClassBrowsing,
	8443: ClassBrowsing,

	// background: system chatter
	53:  ClassBackground, // DNS
	123: ClassBackground, // NTP
}

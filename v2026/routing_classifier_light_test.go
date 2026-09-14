package connect

import (
	"net"
	"net/netip"
	"testing"
)

// fakeServerNameResolver adapts a plain ip-string -> hostname map to
// ServerNameResolver for tests.
func fakeServerNameResolver(names map[string]string) ServerNameResolver {
	return func(ip netip.Addr) (string, bool) {
		name, ok := names[ip.String()]
		return name, ok
	}
}

func testLightIpPath(protocol IpProtocol, destIp string, destPort int) *IpPath {
	return &IpPath{
		Protocol:        protocol,
		SourceIp:        net.ParseIP("192.168.1.2"),
		SourcePort:      51000,
		DestinationIp:   net.ParseIP(destIp),
		DestinationPort: destPort,
	}
}

func TestLightClassifier(t *testing.T) {
	resolver := fakeServerNameResolver(map[string]string{
		"93.184.216.1": "netflix.com",
		"93.184.216.3": "www.bloomberg.com",
	})
	classifier := NewLightClassifier(resolver)

	tests := []struct {
		name           string
		ipPath         *IpPath
		appId          string
		want           TrafficClass
		wantConfidence uint8
	}{
		{
			name:           "Bloomberg video site is streaming",
			ipPath:         testLightIpPath(IpProtocolTcp, "93.184.216.3", 443),
			want:           ClassStreaming,
			wantConfidence: serverNameMatchConfidence,
		},
		{
			name:           "server name beats port default: streaming",
			ipPath:         testLightIpPath(IpProtocolTcp, "93.184.216.1", 443),
			want:           ClassStreaming,
			wantConfidence: serverNameMatchConfidence,
		},
		{
			name:           "no resolvable name falls through to port default: browsing",
			ipPath:         testLightIpPath(IpProtocolTcp, "93.184.216.2", 443),
			want:           ClassBrowsing,
			wantConfidence: portMatchConfidence,
		},
		{
			name:           "stun udp port: latency",
			ipPath:         testLightIpPath(IpProtocolUdp, "8.8.8.8", 3478),
			want:           ClassLatency,
			wantConfidence: portMatchConfidence,
		},
		{
			name:           "bittorrent default port: bulk",
			ipPath:         testLightIpPath(IpProtocolTcp, "10.0.0.5", 51413),
			want:           ClassBulk,
			wantConfidence: portMatchConfidence,
		},
		{
			name:           "unmatched high port with no name and no app never guesses: unknown",
			ipPath:         testLightIpPath(IpProtocolTcp, "10.0.0.6", 54321),
			want:           ClassUnknown,
			wantConfidence: 0,
		},
		{
			name:           "app default beats server name",
			ipPath:         testLightIpPath(IpProtocolTcp, "93.184.216.1", 443),
			appId:          "steam.exe",
			want:           ClassBulk,
			wantConfidence: appMatchConfidence,
		},
		{
			name:           "app match is case-insensitive and strips a full path",
			ipPath:         testLightIpPath(IpProtocolTcp, "93.184.216.2", 443),
			appId:          `C:\Program Files (x86)\Steam\STEAM.EXE`,
			want:           ClassBulk,
			wantConfidence: appMatchConfidence,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifier.Classify(tt.ipPath, tt.appId)
			if got.Class != tt.want {
				t.Fatalf("Classify() class = %v, want %v", got.Class, tt.want)
			}
			if got.Confidence != tt.wantConfidence {
				t.Fatalf("Classify() confidence = %d, want %d", got.Confidence, tt.wantConfidence)
			}
			if got.AppId != tt.appId {
				t.Fatalf("Classify() dropped appId: got %q want %q", got.AppId, tt.appId)
			}
		})
	}
}

// TestLightClassifierNilResolver proves the classifier is safe to construct
// with a nil ServerNameResolver (the install site may not always have one)
// and that it does not panic and still falls through to the port tier.
func TestLightClassifierNilResolver(t *testing.T) {
	classifier := NewLightClassifier(nil)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Classify panicked with nil resolver: %v", r)
		}
	}()

	got := classifier.Classify(testLightIpPath(IpProtocolTcp, "93.184.216.1", 443), "")
	if got.Class != ClassBrowsing {
		t.Fatalf("nil resolver: class = %v, want ClassBrowsing (falls through to port)", got.Class)
	}
}

// TestLightClassifierNilIpPath proves Classify never panics on a nil
// *IpPath (defensive: it is a table lookup on the placement hot path, where a
// panic would be worse than an unknown classification) AND that a nil
// *IpPath does not short-circuit the app tier: appId is the highest-
// precedence signal and never touches ipPath, so a matched app must still
// win even when ipPath is nil. A resolver-only or port-only appId here
// (e.g. "chrome.exe", absent from appClassTable) would let this test pass
// whether or not the tier ordering were correct -- steam.exe is IN the
// table, so this fails if the nil guard short-circuits ahead of the app
// tier.
func TestLightClassifierNilIpPath(t *testing.T) {
	classifier := NewLightClassifier(nil)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Classify panicked with nil IpPath: %v", r)
		}
	}()

	got := classifier.Classify(nil, "steam.exe")
	if got.Class != ClassBulk {
		t.Fatalf("nil IpPath with matched appId: class = %v, want ClassBulk (app tier must not be short-circuited by a nil IpPath)", got.Class)
	}
}

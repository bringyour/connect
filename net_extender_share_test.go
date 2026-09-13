package connect

import (
	"encoding/base64"
	"encoding/hex"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// Share payload tests (EXTENDER.md K7). The payload is what one app writes and
// another reads, so both directions are pinned here: the text form, every
// decode refusal, the selection order the share is built with, and what an
// import leaves in the directory.

// The text form of a share that is deliberately invalid, which `Encode` would
// refuse to produce.
func testExtenderShareText(t *testing.T, share *protocol.ExtenderShare) string {
	t.Helper()
	shareBytes, err := proto.Marshal(share)
	if err != nil {
		t.Fatal(err)
	}
	return ExtenderSharePrefix + base64.RawURLEncoding.EncodeToString(shareBytes)
}

func testExtenderShareIps(t *testing.T, share *protocol.ExtenderShare) []string {
	t.Helper()
	ips := []string{}
	for _, ip := range ExtenderShareAddresses(share) {
		ips = append(ips, ip.String())
	}
	return ips
}

// A share round-trips with and without the settings block, and the text form
// is the prefix plus unpadded base64url (K7).
func TestExtenderShareRoundTrip(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)
	directory.AddBootstrap(netip.MustParseAddr("192.0.2.81"), ExtenderSourceDns)
	directory.AddBootstrap(netip.MustParseAddr("2001:db8::81"), ExtenderSourceDns)

	rootPublicKey := newTestExtenderKey(t)
	rootKeys := NewExtenderRootKeySet(rootPublicKey)

	share := BuildExtenderShare(
		directory,
		testExtenderNetworkHost,
		"extender.space.example",
		"wss://gossip.space.example",
		rootKeys,
		true,
		0,
	)
	text, err := EncodeExtenderShare(share)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(text, "ur-ext:1:") {
		t.Fatalf("payload = %q, want the ur-ext:1: prefix", text)
	}
	if strings.Contains(text, "=") {
		t.Fatalf("payload = %q, want base64url with no padding", text)
	}
	// surrounding whitespace is what a scan, a paste or a chat client adds
	decoded, err := DecodeExtenderShare("\n  " + text + "\t\n")
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Version != ExtenderShareVersion {
		t.Fatalf("version = %d, want %d", decoded.Version, ExtenderShareVersion)
	}
	if decoded.NetworkHost != testExtenderNetworkHost {
		t.Fatalf("network host = %q, want %q", decoded.NetworkHost, testExtenderNetworkHost)
	}
	ips := testExtenderShareIps(t, decoded)
	if !slices.Equal(ips, []string{"192.0.2.81", "2001:db8::81"}) {
		t.Fatalf("addresses = %v, want the v4 and the v6 address", ips)
	}
	// 4 bytes for v4 and 16 for v6, as the wire form says
	if len(decoded.Addresses[0]) != 4 || len(decoded.Addresses[1]) != 16 {
		t.Fatalf(
			"address lengths = %d, %d, want 4 and 16",
			len(decoded.Addresses[0]),
			len(decoded.Addresses[1]),
		)
	}
	if decoded.Settings == nil {
		t.Fatal("the settings block was not carried")
	}
	if decoded.Settings.DnsName != "extender.space.example" {
		t.Fatalf("dns name = %q", decoded.Settings.DnsName)
	}
	if decoded.Settings.GossipUrl != "wss://gossip.space.example" {
		t.Fatalf("gossip url = %q", decoded.Settings.GossipUrl)
	}
	keyHexes := ExtenderShareRootKeyHexes(decoded)
	if !slices.Equal(keyHexes, []string{hex.EncodeToString(rootPublicKey)}) {
		t.Fatalf("root keys = %v, want the one anchor key", keyHexes)
	}

	// without the settings the same addresses travel and nothing else
	plainShare := BuildExtenderShare(
		directory,
		testExtenderNetworkHost,
		"extender.space.example",
		"wss://gossip.space.example",
		rootKeys,
		false,
		0,
	)
	plainText, err := EncodeExtenderShare(plainShare)
	if err != nil {
		t.Fatal(err)
	}
	plainDecoded, err := DecodeExtenderShare(plainText)
	if err != nil {
		t.Fatal(err)
	}
	if plainDecoded.Settings != nil {
		t.Fatalf("settings = %+v, want none", plainDecoded.Settings)
	}
	if plainIps := testExtenderShareIps(t, plainDecoded); !slices.Equal(plainIps, ips) {
		t.Fatalf("addresses = %v, want %v", plainIps, ips)
	}
	if len(plainText) >= len(text) {
		t.Fatal("the payload without settings is not smaller")
	}
}

// Everything the apps refuse, refused once here (K7).
func TestExtenderShareDecodeErrors(t *testing.T) {
	valid := &protocol.ExtenderShare{
		Version:     ExtenderShareVersion,
		NetworkHost: testExtenderNetworkHost,
		Addresses:   [][]byte{{192, 0, 2, 91}},
	}
	if _, err := DecodeExtenderShare(testExtenderShareText(t, valid)); err != nil {
		t.Fatalf("the valid payload did not decode: %v", err)
	}

	manyAddresses := make([][]byte, ExtenderShareMaxAddressCount+1)
	for i := range manyAddresses {
		manyAddresses[i] = []byte{192, 0, 2, byte(i % 256)}
	}
	cases := []struct {
		what string
		text string
	}{
		{what: "no prefix", text: "not-a-share"},
		{
			what: "another scheme",
			text: strings.Replace(testExtenderShareText(t, valid), "ur-ext:1:", "ur-ext:2:", 1),
		},
		{what: "empty", text: ""},
		{what: "not base64", text: ExtenderSharePrefix + "!!!not base64!!!"},
		{what: "not a message", text: ExtenderSharePrefix + base64.RawURLEncoding.EncodeToString([]byte{0xff, 0xff, 0xff})},
		{
			what: "another version",
			text: testExtenderShareText(t, &protocol.ExtenderShare{
				Version:     2,
				NetworkHost: testExtenderNetworkHost,
			}),
		},
		{
			what: "no version",
			text: testExtenderShareText(t, &protocol.ExtenderShare{
				NetworkHost: testExtenderNetworkHost,
			}),
		},
		{
			what: "no network host",
			text: testExtenderShareText(t, &protocol.ExtenderShare{
				Version:   ExtenderShareVersion,
				Addresses: [][]byte{{192, 0, 2, 92}},
			}),
		},
		{
			what: "an address of the wrong length",
			text: testExtenderShareText(t, &protocol.ExtenderShare{
				Version:     ExtenderShareVersion,
				NetworkHost: testExtenderNetworkHost,
				Addresses:   [][]byte{{192, 0, 2}},
			}),
		},
		{
			what: "too many addresses",
			text: testExtenderShareText(t, &protocol.ExtenderShare{
				Version:     ExtenderShareVersion,
				NetworkHost: testExtenderNetworkHost,
				Addresses:   manyAddresses,
			}),
		},
	}
	for _, c := range cases {
		if share, err := DecodeExtenderShare(c.text); err == nil {
			t.Errorf("%s decoded as %+v, want an error", c.what, share)
		}
	}

	// the same rules refuse to encode, so nothing can be written that cannot
	// be read back
	if _, err := EncodeExtenderShare(nil); err == nil {
		t.Error("a missing share encoded")
	}
	if _, err := EncodeExtenderShare(&protocol.ExtenderShare{
		Version:     ExtenderShareVersion,
		NetworkHost: "",
	}); err == nil {
		t.Error("a share with no network host encoded")
	}
	// the version is filled in when the caller leaves it out
	filled, err := EncodeExtenderShare(&protocol.ExtenderShare{
		NetworkHost: testExtenderNetworkHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	if share, err := DecodeExtenderShare(filled); err != nil {
		t.Fatal(err)
	} else if share.Version != ExtenderShareVersion {
		t.Fatalf("version = %d, want %d", share.Version, ExtenderShareVersion)
	}
}

// Addresses are selected active first, then usable, then manual, and the count
// is capped (K7).
func TestExtenderShareSelectionOrderAndCap(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)

	activeIp := netip.MustParseAddr("192.0.2.104")
	usableIp := netip.MustParseAddr("192.0.2.102")
	otherUsableIp := netip.MustParseAddr("192.0.2.103")
	manualHeldIp := netip.MustParseAddr("192.0.2.101")
	heldIp := netip.MustParseAddr("192.0.2.100")

	directory.AddBootstrap(activeIp, ExtenderSourceDns)
	directory.AddBootstrap(usableIp, ExtenderSourceDns)
	directory.AddBootstrap(otherUsableIp, ExtenderSourceDns)
	directory.AddManual(manualHeldIp)
	directory.AddBootstrap(heldIp, ExtenderSourceDns)

	// carrying a connection right now
	directory.SetInUse(activeIp, 1)
	// on hold: only the manual one is worth sharing, because it is the one the
	// importer cannot rediscover
	directory.RecordFailure(manualHeldIp, ExtenderConnectModeTcpTls)
	directory.RecordFailure(heldIp, ExtenderConnectModeTcpTls)

	share := BuildExtenderShare(directory, testExtenderNetworkHost, "", "", nil, false, 0)
	ips := testExtenderShareIps(t, share)
	expect := []string{
		activeIp.String(),
		usableIp.String(),
		otherUsableIp.String(),
		manualHeldIp.String(),
	}
	if !slices.Equal(ips, expect) {
		t.Fatalf("addresses = %v, want %v", ips, expect)
	}

	// the cap takes the head of that order
	capped := BuildExtenderShare(directory, testExtenderNetworkHost, "", "", nil, false, 2)
	if cappedIps := testExtenderShareIps(t, capped); !slices.Equal(cappedIps, expect[0:2]) {
		t.Fatalf("capped addresses = %v, want %v", cappedIps, expect[0:2])
	}
	// an oversized bound is clamped to what a share may carry, so a built
	// share always encodes
	oversized := BuildExtenderShare(directory, testExtenderNetworkHost, "", "", nil, false, 10000)
	if ExtenderShareMaxAddressCount < len(oversized.Addresses) {
		t.Fatalf("addresses = %d, want at most %d", len(oversized.Addresses), ExtenderShareMaxAddressCount)
	}
	if _, err := EncodeExtenderShare(oversized); err != nil {
		t.Fatalf("a share built with an oversized bound did not encode: %v", err)
	}
}

// An import adds every address as an unverified entry with the import source,
// and adds nothing the directory already knows (K7).
func TestExtenderShareApplyCreatesImportEntries(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)
	knownIp := netip.MustParseAddr("192.0.2.111")
	directory.AddBootstrap(knownIp, ExtenderSourceDns)

	share := &protocol.ExtenderShare{
		Version:     ExtenderShareVersion,
		NetworkHost: testExtenderNetworkHost,
		Addresses: [][]byte{
			{192, 0, 2, 111},
			{192, 0, 2, 112},
			netip.MustParseAddr("2001:db8::112").AsSlice(),
		},
	}
	if added := ApplyExtenderShare(directory, share, ""); added != 2 {
		t.Fatalf("added = %d, want the two new addresses", added)
	}
	for _, ip := range []netip.Addr{
		netip.MustParseAddr("192.0.2.112"),
		netip.MustParseAddr("2001:db8::112"),
	} {
		entry := testDirectoryEntry(t, directory, ip)
		if entry.Source != ExtenderSourceImport {
			t.Fatalf("%s source = %s, want import", ip, entry.Source)
		}
		if entry.State != ExtenderStateUnverified {
			t.Fatalf("%s state = %s, want unverified", ip, entry.State)
		}
	}
	// an address the directory already knows keeps where it came from
	if entry := testDirectoryEntry(t, directory, knownIp); entry.Source != ExtenderSourceDns {
		t.Fatalf("known source = %s, want dns", entry.Source)
	}
	// applying the same share again adds nothing
	if added := ApplyExtenderShare(directory, share, ""); added != 0 {
		t.Fatalf("added = %d on a second apply, want 0", added)
	}
	// a caller may name its own source
	otherShare := &protocol.ExtenderShare{
		Version:     ExtenderShareVersion,
		NetworkHost: testExtenderNetworkHost,
		Addresses:   [][]byte{{192, 0, 2, 113}},
	}
	if added := ApplyExtenderShare(directory, otherShare, ExtenderSourceBootstrap); added != 1 {
		t.Fatalf("added = %d, want 1", added)
	}
	if entry := testDirectoryEntry(t, directory, netip.MustParseAddr("192.0.2.113")); entry.Source != ExtenderSourceBootstrap {
		t.Fatalf("source = %s, want bootstrap", entry.Source)
	}
	if added := ApplyExtenderShare(nil, otherShare, ""); added != 0 {
		t.Fatalf("added = %d with no directory, want 0", added)
	}
}

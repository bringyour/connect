package connect

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// The edges of the share payload, the feed codec and the probe arguments
// (EXTENDER.md K7, D4, C2).
//
// A share is the one payload a stranger hands the app, and the feed frame is
// the one the extender hands the client, so both are bounded and refused
// rather than trusted. The probe arguments are refused before anything is
// dialed, so a malformed activation row costs no connection.

// A share is refused on the way out for the same reasons it is refused on the
// way in, so nothing can be written that a reader must then reject.
func TestExtenderShareEncodeValidatesBeforeItWrites(t *testing.T) {
	addresses := [][]byte{netip.MustParseAddr("192.0.2.10").AsSlice()}
	cases := []struct {
		name  string
		share *protocol.ExtenderShare
	}{
		{
			name: "another version",
			share: &protocol.ExtenderShare{
				Version:     ExtenderShareVersion + 1,
				NetworkHost: testExtenderNetworkHost,
				Addresses:   addresses,
			},
		},
		{
			name: "no network host",
			share: &protocol.ExtenderShare{
				Version:   ExtenderShareVersion,
				Addresses: addresses,
			},
		},
		{
			name: "an address that is neither 4 nor 16 bytes",
			share: &protocol.ExtenderShare{
				Version:     ExtenderShareVersion,
				NetworkHost: testExtenderNetworkHost,
				Addresses:   [][]byte{{1, 2, 3}},
			},
		},
		{
			name: "over the address cap",
			share: &protocol.ExtenderShare{
				Version:     ExtenderShareVersion,
				NetworkHost: testExtenderNetworkHost,
				Addresses:   testShareAddressBytes(ExtenderShareMaxAddressCount + 1),
			},
		},
	}
	for _, c := range cases {
		if text, err := EncodeExtenderShare(c.share); err == nil {
			t.Errorf("%s encoded as %q", c.name, text)
		}
	}

	// exactly at the cap is still written, and reads back
	atCap := &protocol.ExtenderShare{
		Version:     ExtenderShareVersion,
		NetworkHost: testExtenderNetworkHost,
		Addresses:   testShareAddressBytes(ExtenderShareMaxAddressCount),
	}
	text, err := EncodeExtenderShare(atCap)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeExtenderShare(text)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Addresses) != ExtenderShareMaxAddressCount {
		t.Fatalf("the share carries %d addresses", len(decoded.Addresses))
	}
}

// The version is stamped on a copy, so a caller that shares one message twice
// does not have it rewritten underneath.
func TestExtenderShareEncodeDoesNotMutateTheCaller(t *testing.T) {
	share := &protocol.ExtenderShare{
		NetworkHost: testExtenderNetworkHost,
		Addresses:   [][]byte{netip.MustParseAddr("192.0.2.10").AsSlice()},
	}
	if _, err := EncodeExtenderShare(share); err != nil {
		t.Fatal(err)
	}
	if share.Version != 0 {
		t.Fatalf("the caller's share was stamped version %d", share.Version)
	}
}

// A share read from anywhere is safe to walk: a malformed address is skipped
// rather than parsed short, and a v4-mapped address comes back as the v4 form
// so it keys the same directory entry.
func TestExtenderShareAddressesSkipAndUnmap(t *testing.T) {
	mapped := netip.MustParseAddr("::ffff:192.0.2.10").As16()
	share := &protocol.ExtenderShare{
		Version:     ExtenderShareVersion,
		NetworkHost: testExtenderNetworkHost,
		Addresses: [][]byte{
			{1, 2, 3},
			{},
			nil,
			mapped[:],
			netip.MustParseAddr("2001:db8::10").AsSlice(),
			make([]byte, 5),
		},
	}
	ips := ExtenderShareAddresses(share)
	want := []string{"192.0.2.10", "2001:db8::10"}
	got := []string{}
	for _, ip := range ips {
		got = append(got, ip.String())
	}
	if !slices.Equal(got, want) {
		t.Fatalf("addresses = %v, expected %v", got, want)
	}
	if ips := ExtenderShareAddresses(nil); ips != nil {
		t.Errorf("a nil share yielded %v", ips)
	}
}

// The wire form of an address is the family's own width, and a v4-mapped
// address is written as the v4 form so two shares of one host are one entry.
func TestExtenderShareAddressBytesFollowTheFamily(t *testing.T) {
	cases := []struct {
		ip        string
		byteCount int
	}{
		{ip: "192.0.2.10", byteCount: 4},
		{ip: "::ffff:192.0.2.10", byteCount: 4},
		{ip: "2001:db8::10", byteCount: 16},
	}
	for _, c := range cases {
		addressBytes := extenderShareAddressBytes(netip.MustParseAddr(c.ip))
		if len(addressBytes) != c.byteCount {
			t.Errorf("%s is %d bytes, expected %d", c.ip, len(addressBytes), c.byteCount)
		}
	}
	var invalid netip.Addr
	if addressBytes := extenderShareAddressBytes(invalid); addressBytes != nil {
		t.Errorf("an invalid address is %d bytes", len(addressBytes))
	}
}

// The settings block is optional at every step: a share without one carries no
// keys to read, and an empty key inside one is skipped rather than decoded to
// an empty anchor entry.
func TestExtenderShareRootKeyHexesSkipEmptyKeys(t *testing.T) {
	_, publicKey := newTestRootKey(t)
	share := &protocol.ExtenderShare{
		Version:     ExtenderShareVersion,
		NetworkHost: testExtenderNetworkHost,
		Settings: &protocol.ExtenderShareSettings{
			RootPublicKeys: [][]byte{{}, publicKey, nil},
		},
	}
	keyHexes := ExtenderShareRootKeyHexes(share)
	if !slices.Equal(keyHexes, []string{hex.EncodeToString(publicKey)}) {
		t.Fatalf("key hexes = %v", keyHexes)
	}
	if keyHexes := ExtenderShareRootKeyHexes(nil); keyHexes != nil {
		t.Errorf("a nil share yielded %v", keyHexes)
	}
	noSettings := &protocol.ExtenderShare{Version: ExtenderShareVersion}
	if keyHexes := ExtenderShareRootKeyHexes(noSettings); keyHexes != nil {
		t.Errorf("a share with no settings yielded %v", keyHexes)
	}
}

// A share built without a directory is the settings block alone, which is how
// a space with nothing discovered yet still hands over its trust anchor, and
// the default address bound applies when the caller names none.
func TestExtenderShareBuildWithoutADirectory(t *testing.T) {
	_, publicKey := newTestRootKey(t)
	share := BuildExtenderShare(
		nil,
		testExtenderNetworkHost,
		"extender.space.example",
		"wss://gossip.space.example",
		NewExtenderRootKeySet(publicKey),
		true,
		0,
	)
	if share.NetworkHost != testExtenderNetworkHost {
		t.Errorf("network host = %q", share.NetworkHost)
	}
	if 0 < len(share.Addresses) {
		t.Errorf("a directoryless share carries %d addresses", len(share.Addresses))
	}
	if share.Settings == nil {
		t.Fatal("the settings block was dropped")
	}
	if share.Settings.DnsName != "extender.space.example" {
		t.Errorf("dns name = %q", share.Settings.DnsName)
	}
	if share.Settings.GossipUrl != "wss://gossip.space.example" {
		t.Errorf("gossip url = %q", share.Settings.GossipUrl)
	}
	if len(share.Settings.RootPublicKeys) != 1 {
		t.Fatalf("the settings carry %d keys", len(share.Settings.RootPublicKeys))
	}

	// nil root keys are an empty list rather than a nil one, so an importer
	// that takes the settings replaces the anchor with nothing rather than
	// reading a nil field
	noKeys := BuildExtenderShare(nil, testExtenderNetworkHost, "", "", nil, true, 0)
	if noKeys.Settings == nil || noKeys.Settings.RootPublicKeys == nil {
		t.Fatal("a share with no root keys dropped the key list")
	}
	if 0 < len(noKeys.Settings.RootPublicKeys) {
		t.Errorf("the settings carry %d keys", len(noKeys.Settings.RootPublicKeys))
	}
	// without the settings flag the block is absent entirely
	plain := BuildExtenderShare(nil, testExtenderNetworkHost, "x", "y", nil, false, 0)
	if plain.Settings != nil {
		t.Error("a share without settings carried a settings block")
	}
}

// The default address bound is taken when the caller names none, so a share
// built with no bound is the documented size rather than the whole directory.
func TestExtenderShareTakesTheDefaultAddressBound(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
		settings.MaxAddressCount = 0
	})
	for i := 0; i < ExtenderShareDefaultAddressCount+8; i += 1 {
		directory.AddBootstrap(
			netip.AddrFrom4([4]byte{192, 0, 2, byte(i + 1)}), ExtenderSourceDns)
	}
	share := BuildExtenderShare(
		directory, testExtenderNetworkHost, "", "", nil, false, 0)
	if len(share.Addresses) != ExtenderShareDefaultAddressCount {
		t.Fatalf(
			"the share carries %d addresses, expected the default %d",
			len(share.Addresses), ExtenderShareDefaultAddressCount)
	}
	// two builds of one directory are the same payload, so a share is stable
	again := BuildExtenderShare(directory, testExtenderNetworkHost, "", "", nil, false, 0)
	for i := range share.Addresses {
		if !bytes.Equal(share.Addresses[i], again.Addresses[i]) {
			t.Fatalf("two builds of one directory differ at %d", i)
		}
	}
}

// Applying a share to nothing, or applying nothing, is a no-op rather than a
// panic, which is what an import into a space with no directory looks like.
func TestExtenderShareApplyToleratesNothing(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)
	if added := ApplyExtenderShare(nil, nil, ""); added != 0 {
		t.Errorf("applying nothing added %d", added)
	}
	if added := ApplyExtenderShare(directory, nil, ""); added != 0 {
		t.Errorf("applying a nil share added %d", added)
	}
	if 0 < len(directory.Snapshot().Entries) {
		t.Error("the directory grew")
	}
}

// The feed codec refuses a missing message on the way out and an over-cap
// length on the way in, so neither end is made to allocate on the other's word
// (D4).
func TestExtenderFeedCodecRefusals(t *testing.T) {
	buffer := &bytes.Buffer{}
	if err := WriteExtenderFeedRequest(buffer, nil); err == nil {
		t.Error("a nil request was written")
	}
	if err := WriteExtenderFeedFrame(buffer, nil); err == nil {
		t.Error("a nil frame was written")
	}
	if 0 < buffer.Len() {
		t.Fatalf("a refused write left %d bytes on the stream", buffer.Len())
	}

	// a truncated payload is the read error, not a short frame
	if err := WriteExtenderFeedFrame(buffer, &protocol.ExtenderFeedFrame{
		Frame: &protocol.ExtenderFeedFrame_EndOfSample{EndOfSample: true},
	}); err != nil {
		t.Fatal(err)
	}
	frameBytes := buffer.Bytes()
	if _, err := ReadExtenderFeedFrame(
		bytes.NewReader(frameBytes[:len(frameBytes)-1]),
	); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a truncated frame returned %v", err)
	}
	// a body that is not a frame does not decode
	badBody := []byte{0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff}
	if _, err := ReadExtenderFeedFrame(bytes.NewReader(badBody)); err == nil {
		t.Error("a body that is not a frame decoded")
	}
	if _, err := ReadExtenderFeedRequest(bytes.NewReader(badBody)); err == nil {
		t.Error("a body that is not a request decoded")
	}
	// a clean end of stream is io.EOF, which is how a reader tells a closed
	// subscription from a broken one
	if _, err := ReadExtenderFeedFrame(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Errorf("an empty stream returned %v", err)
	}
}

// The feed stream closes once however many times it is closed, so a reader
// that closes on its error path and again in a defer releases one carrier.
func TestExtenderFeedStreamCloseIsOnce(t *testing.T) {
	stream, serverConn := newTestFeedStream(t)
	defer serverConn.Close()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("the second close returned %v", err)
	}
	// the stream is gone, so the next read fails rather than blocking
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if frame, err := stream.Next(ctx); err == nil {
		t.Fatalf("a closed stream yielded %v", frame)
	}
}

// Both probes refuse their arguments before anything is dialed, so a malformed
// activation row never opens a socket (C2).
func TestExtenderProbeRefusesItsArgumentsBeforeDialing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connectSettings := DefaultConnectSettings()
	var invalid netip.Addr

	if _, err := ProbeExtenderCarrier(
		ctx,
		connectSettings,
		invalid,
		ExtenderConnectModeTcpTls,
		443,
		DefaultExtenderDnsTld,
		"spoof.example",
		nil,
		"api.space.example",
		443,
	); err == nil {
		t.Error("an invalid address was probed")
	} else if !strings.Contains(err.Error(), "not valid") {
		t.Errorf("the invalid address failed with %v", err)
	}

	if _, err := ProbeExtenderCarrier(
		ctx,
		connectSettings,
		netip.MustParseAddr("192.0.2.10"),
		ExtenderConnectModeTcpTls,
		443,
		DefaultExtenderDnsTld,
		"spoof.example",
		nil,
		"",
		443,
	); err == nil {
		t.Error("a probe with no destination was dialed")
	} else if !strings.Contains(err.Error(), "no destination") {
		t.Errorf("the empty destination failed with %v", err)
	}

	if _, err := ProbeExtenderForward(
		ctx,
		connectSettings,
		invalid,
		443,
		"spoof.example",
		nil,
		"https://api.space.example",
		nil,
	); err == nil {
		t.Error("an invalid address was probed for the forward")
	}
}

// A probe is bounded by the request timeout, so a black hole costs a known
// amount of time rather than the caller's whole context.
func TestExtenderProbeContextBoundsTheRequest(t *testing.T) {
	connectSettings := DefaultConnectSettings()
	connectSettings.RequestTimeout = time.Minute
	probeCtx, probeCancel := probeContext(context.Background(), connectSettings)
	defer probeCancel()
	deadline, ok := probeCtx.Deadline()
	if !ok {
		t.Fatal("the probe context carries no deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 || time.Minute < remaining {
		t.Fatalf("the probe budget is %s", remaining)
	}

	// without a request timeout the caller's context is the only bound
	connectSettings.RequestTimeout = 0
	callerCtx, callerCancel := context.WithCancel(context.Background())
	plainCtx, plainCancel := probeContext(callerCtx, connectSettings)
	defer plainCancel()
	if _, ok := plainCtx.Deadline(); ok {
		t.Error("an unbounded probe carries a deadline")
	}
	callerCancel()
	<-plainCtx.Done()
}

// The feed caps the design names, pinned so a change to either is deliberate.
func TestExtenderFeedCountsAreTheDesignBounds(t *testing.T) {
	if ExtenderFeedMaxFrameByteCount != 64*1024 {
		t.Errorf("the frame cap is %d", ExtenderFeedMaxFrameByteCount)
	}
	if DefaultExtenderFeedSampleCount != 16 {
		t.Errorf("the default sample is %d", DefaultExtenderFeedSampleCount)
	}
	if ExtenderFeedMaxSampleCount != 32 {
		t.Errorf("the sample cap is %d", ExtenderFeedMaxSampleCount)
	}
	if ExtenderShareDefaultAddressCount != 48 {
		t.Errorf("the default share size is %d", ExtenderShareDefaultAddressCount)
	}
	if ExtenderShareMaxAddressCount != 256 {
		t.Errorf("the share cap is %d", ExtenderShareMaxAddressCount)
	}
	if ExtenderSharePrefix != "ur-ext:1:" {
		t.Errorf("the share prefix is %q", ExtenderSharePrefix)
	}
	if ExtenderMaxHeaderByteCount != 1024 {
		t.Errorf("the header cap is %d", ExtenderMaxHeaderByteCount)
	}
	if ExtenderChallengeByteCount != 32 {
		t.Errorf("the challenge is %d bytes", ExtenderChallengeByteCount)
	}
	if ExtenderKeyIdByteCount != 8 {
		t.Errorf("the key id is %d bytes", ExtenderKeyIdByteCount)
	}
	if ExtenderDirectoryEventRingCount != 1024 {
		t.Errorf("the event ring is %d", ExtenderDirectoryEventRingCount)
	}
	if ExtenderDirectorySubscribeBufferCount != 64 {
		t.Errorf("the subscribe buffer is %d", ExtenderDirectorySubscribeBufferCount)
	}
	if extenderHelloMaxByteCount != 64*1024 {
		t.Errorf("the hello read ceiling is %d", extenderHelloMaxByteCount)
	}
	// the fixed carrier ports of A1 and L2
	if ExtenderTcpPort != 443 || ExtenderQuicPort != 443 {
		t.Errorf("the tcp and quic ports are %d and %d", ExtenderTcpPort, ExtenderQuicPort)
	}
	if ExtenderDnsPort != DefaultWhodisPort || DefaultWhodisPort != 4053 {
		t.Errorf("the dns port is %d", ExtenderDnsPort)
	}
	if DefaultDnsPort != 53 {
		t.Errorf("the privileged dns port is %d", DefaultDnsPort)
	}
	if DefaultAltH3Port != 443 {
		t.Errorf("the alt h3 port is %d", DefaultAltH3Port)
	}
}

// The signature domains are distinct strings, so a signature made for one can
// never be read as another (B2).
func TestExtenderSignatureDomainsAreDistinct(t *testing.T) {
	domains := []string{
		ExtenderRecordSignatureDomain,
		ExtenderRevocationSignatureDomain,
		ExtenderChallengeSignatureDomain,
	}
	want := []string{
		"ur-extender-record-v1",
		"ur-extender-revocation-v1",
		"ur-extender-challenge-v1",
	}
	if !slices.Equal(domains, want) {
		t.Fatalf("domains = %v, expected %v", domains, want)
	}
	seen := map[string]bool{}
	for _, domain := range domains {
		if seen[domain] {
			t.Errorf("%q is used twice", domain)
		}
		seen[domain] = true
	}
	// the services of A4, which the extender dispatches on
	if ExtenderServiceForward != 0 || ExtenderServiceGossip != 1 || ExtenderServiceFeed != 2 {
		t.Errorf(
			"services are %d/%d/%d",
			ExtenderServiceForward, ExtenderServiceGossip, ExtenderServiceFeed)
	}
	if ExtenderContentType != "application/x-ur-extender" {
		t.Errorf("the content type is %q", ExtenderContentType)
	}
	if ExtenderActivatePath != "/network/extender-activate" {
		t.Errorf("the activate path is %q", ExtenderActivatePath)
	}
}

// Address payloads of a share, one per synthetic host.
func testShareAddressBytes(count int) [][]byte {
	addresses := [][]byte{}
	for i := 0; i < count; i += 1 {
		ip := netip.AddrFrom16([16]byte{
			0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, byte(i >> 8), byte(i),
		})
		addresses = append(addresses, ip.AsSlice())
	}
	return addresses
}

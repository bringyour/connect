package connect

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// The extender share payload (EXTENDER.md K7).
//
// A share carries addresses and nothing else: no keys and no records, because
// an imported address is an unverified bootstrap entry that upgrades the
// moment a signed record naming it arrives over the feed. That is what bounds
// a hostile code -- the worst it can do is add addresses that never verify and
// are aged out by the ordinary failure policy.
//
// The optional settings block is the exception: it replaces the dns name, the
// gossip url and the trust anchor, so an importer takes it only deliberately,
// and the first hello over the platform's pinned TLS replaces the root keys
// again.
//
// The text form is one line, `ur-ext:1:` and the serialized message in
// base64url with no padding, so it survives a qr code, a chat message and a
// copy/paste with surrounding whitespace.
//
// Encoding, decoding and building are pure functions of their arguments and
// are safe for concurrent use.

const (
	// The text prefix, which carries the version so a reader can refuse a
	// payload it does not understand before decoding anything.
	ExtenderSharePrefix = "ur-ext:1:"
	// The only version this release writes and accepts.
	ExtenderShareVersion = 1
	// Addresses in one share when the caller names no bound (K7).
	ExtenderShareDefaultAddressCount = 48
	// Hard cap. A payload naming more than this is refused rather than
	// applied: a share is a bootstrap hint, not a directory transfer.
	ExtenderShareMaxAddressCount = 256
)

// Serializes a share as the text form above. The share is validated first, so
// a payload this returns always decodes.
func EncodeExtenderShare(share *protocol.ExtenderShare) (string, error) {
	if share == nil {
		return "", fmt.Errorf("the extender share is missing")
	}
	if share.Version == 0 {
		// the version is the payload's own, not the caller's to forget
		share = proto.Clone(share).(*protocol.ExtenderShare)
		share.Version = ExtenderShareVersion
	}
	if err := validateExtenderShare(share); err != nil {
		return "", err
	}
	shareBytes, err := proto.Marshal(share)
	if err != nil {
		return "", err
	}
	return ExtenderSharePrefix + base64.RawURLEncoding.EncodeToString(shareBytes), nil
}

// Reads the text form, tolerating the whitespace a scan, a paste or a chat
// client adds around it. Everything the apps refuse is refused here, once: a
// foreign version, a share that names no network host, an address that is not
// 4 or 16 bytes, and a share over the address cap.
func DecodeExtenderShare(text string) (*protocol.ExtenderShare, error) {
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, ExtenderSharePrefix) {
		return nil, fmt.Errorf("the extender share does not start with %s", ExtenderSharePrefix)
	}
	encoded := strings.TrimSpace(strings.TrimPrefix(trimmed, ExtenderSharePrefix))
	shareBytes, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	share := &protocol.ExtenderShare{}
	if err := proto.Unmarshal(shareBytes, share); err != nil {
		return nil, err
	}
	if err := validateExtenderShare(share); err != nil {
		return nil, err
	}
	return share, nil
}

// The rules both directions judge by, so nothing can be written that cannot be
// read back.
func validateExtenderShare(share *protocol.ExtenderShare) error {
	if share == nil {
		return fmt.Errorf("the extender share is missing")
	}
	if share.Version != ExtenderShareVersion {
		return fmt.Errorf("the extender share is version %d", share.Version)
	}
	if share.NetworkHost == "" {
		return fmt.Errorf("the extender share names no network host")
	}
	if ExtenderShareMaxAddressCount < len(share.Addresses) {
		return fmt.Errorf(
			"the extender share carries %d addresses, at most %d",
			len(share.Addresses),
			ExtenderShareMaxAddressCount,
		)
	}
	for _, addressBytes := range share.Addresses {
		switch len(addressBytes) {
		case 4, 16:
		default:
			return fmt.Errorf("an extender share address is %d bytes", len(addressBytes))
		}
	}
	return nil
}

// Builds a share from the directory (K7). At most `maxCount` addresses are
// taken, active first -- the extenders carrying a connection right now -- then
// the rest of the usable ones, then the manual addresses that are neither, so
// a small share is the most useful set rather than an arbitrary one. Keys and
// records are never shared. `includeSettings` adds the operator block, which
// an importer applies only when it asks to.
func BuildExtenderShare(
	directory *ExtenderDirectory,
	networkHost string,
	dnsName string,
	gossipUrl string,
	rootKeys *ExtenderRootKeySet,
	includeSettings bool,
	maxCount int,
) *protocol.ExtenderShare {
	if maxCount <= 0 {
		maxCount = ExtenderShareDefaultAddressCount
	}
	maxCount = min(maxCount, ExtenderShareMaxAddressCount)

	share := &protocol.ExtenderShare{
		Version:     ExtenderShareVersion,
		NetworkHost: networkHost,
		Addresses:   [][]byte{},
	}
	if includeSettings {
		settings := &protocol.ExtenderShareSettings{
			DnsName:        dnsName,
			GossipUrl:      gossipUrl,
			RootPublicKeys: [][]byte{},
		}
		if rootKeys != nil {
			for _, publicKey := range rootKeys.PublicKeys() {
				settings.RootPublicKeys = append(settings.RootPublicKeys, slices.Clone(publicKey))
			}
		}
		share.Settings = settings
	}
	if directory == nil {
		return share
	}

	// the tier of one entry, lowest first; -1 is not shared at all. A revoked,
	// expired or held address is worth sharing only when it was configured by
	// hand, because that is the one kind the importer cannot rediscover.
	tier := func(entry *ExtenderDirectoryEntry) int {
		if 0 < entry.InUse {
			return 0
		}
		switch entry.State {
		case ExtenderStateActive, ExtenderStateUnverified, ExtenderStateWarning:
			return 1
		}
		if entry.Source == ExtenderSourceManual {
			return 2
		}
		return -1
	}
	// the snapshot is already ordered by address, so the sort inside a tier is
	// stable and two shares of the same directory are the same payload
	entries := []*ExtenderDirectoryEntry{}
	for _, entry := range directory.Snapshot().Entries {
		if 0 <= tier(entry) {
			entries = append(entries, entry)
		}
	}
	slices.SortStableFunc(entries, func(a *ExtenderDirectoryEntry, b *ExtenderDirectoryEntry) int {
		return tier(a) - tier(b)
	})
	for _, entry := range entries {
		if maxCount <= len(share.Addresses) {
			break
		}
		if addressBytes := extenderShareAddressBytes(entry.Ip); addressBytes != nil {
			share.Addresses = append(share.Addresses, addressBytes)
		}
	}
	return share
}

// Adds every address of a share to the directory as an unverified bootstrap
// entry (K7). An empty source is `import`, which is what a scanned or pasted
// payload is. Returns how many addresses were new; an address already known
// keeps everything it has, including where it came from.
func ApplyExtenderShare(
	directory *ExtenderDirectory,
	share *protocol.ExtenderShare,
	source string,
) int {
	if directory == nil || share == nil {
		return 0
	}
	if source == "" {
		source = ExtenderSourceImport
	}
	added := 0
	for _, ip := range ExtenderShareAddresses(share) {
		if directory.AddBootstrap(ip, source) {
			added += 1
		}
	}
	return added
}

// The addresses of a share as parsed values, skipping anything malformed so a
// share that was not decoded by `DecodeExtenderShare` is still safe to read.
func ExtenderShareAddresses(share *protocol.ExtenderShare) []netip.Addr {
	if share == nil {
		return nil
	}
	ips := []netip.Addr{}
	for _, addressBytes := range share.Addresses {
		switch len(addressBytes) {
		case 4, 16:
		default:
			continue
		}
		ip, ok := netip.AddrFromSlice(addressBytes)
		if !ok || !ip.IsValid() {
			continue
		}
		ips = append(ips, ip.Unmap())
	}
	return ips
}

// The share settings' root keys in the hex form the network space values and
// the directory anchor are configured with (B4), so an importer that takes the
// settings does not re-encode them itself.
func ExtenderShareRootKeyHexes(share *protocol.ExtenderShare) []string {
	if share == nil || share.Settings == nil {
		return nil
	}
	keyHexes := []string{}
	for _, publicKey := range share.Settings.RootPublicKeys {
		if len(publicKey) == 0 {
			continue
		}
		keyHexes = append(keyHexes, hex.EncodeToString(publicKey))
	}
	return keyHexes
}

// The wire form of one address: 4 bytes for v4, 16 for v6, nil for anything
// that is neither.
func extenderShareAddressBytes(ip netip.Addr) []byte {
	ip = ip.Unmap()
	switch {
	case ip.Is4():
		addressBytes := ip.As4()
		return addressBytes[:]
	case ip.Is6():
		addressBytes := ip.As16()
		return addressBytes[:]
	default:
		return nil
	}
}

package connect

// ip_family.go — the address-family vocabulary shared by discovery
// (find-providers2), the multi-client windows, the window monitor, the sdk
// and the family-pinned platform transports. See IPV6.md §3.
//
// Two distinct types live here. `IpFamily` is a provider's proven egress
// category as the platform categorizes it from control connections whose
// declared and observed families agree. `IpFamilyFilter` is what a client
// asks find-providers2 for. They are kept apart because a filter names a
// capability ("anything that can carry v6") while a category names a fact
// about one provider, and the two vocabularies only partly overlap.
//
// The empty `IpFamily` is LEGACY, not unknown: an older server that does not
// send the field, or a generator that predates it, describes providers the
// network has always assumed can egress v4. Treating legacy as v4-only is what
// keeps an old client, an old server and an old cache entry all behaving
// exactly as they do today.

// IpFamily is a provider's proven address-family category.
type IpFamily string

const (
	// IpFamilyLegacy is the zero value: no category was reported. Behaves as
	// v4-only everywhere.
	IpFamilyLegacy IpFamily = ""
	// IpFamilyDualstack means both families were proven.
	IpFamilyDualstack IpFamily = "dualstack"
	// IpFamilyV4Only means only v4 was proven.
	IpFamilyV4Only IpFamily = "v4-only"
	// IpFamilyV6Only means only v6 was proven.
	IpFamilyV6Only IpFamily = "v6-only"
)

// Normalize maps a value this build does not know to legacy, so a category a
// newer server introduces degrades to today's behavior instead of to "carries
// nothing".
func (self IpFamily) Normalize() IpFamily {
	switch self {
	case IpFamilyDualstack, IpFamilyV4Only, IpFamilyV6Only:
		return self
	default:
		return IpFamilyLegacy
	}
}

// SupportsIpv4 reports whether an exit in this category can carry v4 flows.
func (self IpFamily) SupportsIpv4() bool {
	switch self.Normalize() {
	case IpFamilyDualstack, IpFamilyV4Only, IpFamilyLegacy:
		return true
	default:
		return false
	}
}

// SupportsIpv6 reports whether an exit in this category can carry v6 flows.
func (self IpFamily) SupportsIpv6() bool {
	switch self.Normalize() {
	case IpFamilyDualstack, IpFamilyV6Only:
		return true
	default:
		return false
	}
}

// SupportsIpVersion is the packet-path form: `ipVersion` is the 4 or 6 read
// from the packet header. Any other version is not carried.
func (self IpFamily) SupportsIpVersion(ipVersion int) bool {
	switch ipVersion {
	case 4:
		return self.SupportsIpv4()
	case 6:
		return self.SupportsIpv6()
	default:
		return false
	}
}

// Label is the short user-facing form used by the apps' histogram and
// provider rows: "both", "v4" or "v6". Legacy reads as "v4" because that is
// what it carries.
func (self IpFamily) Label() string {
	switch self.Normalize() {
	case IpFamilyDualstack:
		return "both"
	case IpFamilyV6Only:
		return "v6"
	default:
		return "v4"
	}
}

// IpFamilyFilter is the find-providers2 `ip_family` request filter.
type IpFamilyFilter string

const (
	// IpFamilyFilterDefault is the empty filter. The server treats it as
	// `IpFamilyFilterV4Capable`, which is every provider an older server
	// knows, so an older client keeps today's behavior.
	IpFamilyFilterDefault IpFamilyFilter = ""
	// IpFamilyFilterV4Capable selects dualstack first, then v4-only.
	IpFamilyFilterV4Capable IpFamilyFilter = "v4-capable"
	// IpFamilyFilterV6Capable selects dualstack first, then v6-only.
	IpFamilyFilterV6Capable IpFamilyFilter = "v6-capable"
	// IpFamilyFilterDualstack selects exactly the dualstack category.
	IpFamilyFilterDualstack IpFamilyFilter = "dualstack"
	// IpFamilyFilterV4Only selects exactly the v4-only category.
	IpFamilyFilterV4Only IpFamilyFilter = "v4-only"
	// IpFamilyFilterV6Only selects exactly the v6-only category.
	IpFamilyFilterV6Only IpFamilyFilter = "v6-only"
)

// Matches reports whether a provider of `family` satisfies this filter. It is
// the client-side mirror of the server's selection, used by local generators
// and tests. An unknown filter matches nothing, which is the fail-closed
// reading: a filter this build cannot interpret must not hand a flow an exit
// that may not carry it.
func (self IpFamilyFilter) Matches(family IpFamily) bool {
	family = family.Normalize()
	switch self {
	case IpFamilyFilterDefault, IpFamilyFilterV4Capable:
		return family.SupportsIpv4()
	case IpFamilyFilterV6Capable:
		return family.SupportsIpv6()
	case IpFamilyFilterDualstack:
		return family == IpFamilyDualstack
	case IpFamilyFilterV4Only:
		return family == IpFamilyV4Only || family == IpFamilyLegacy
	case IpFamilyFilterV6Only:
		return family == IpFamilyV6Only
	default:
		return false
	}
}

// IpFamilyFilterForIpVersion is the capability filter that can carry flows of
// the given packet version.
func IpFamilyFilterForIpVersion(ipVersion int) IpFamilyFilter {
	if ipVersion == 6 {
		return IpFamilyFilterV6Capable
	}
	return IpFamilyFilterV4Capable
}

// HeaderIpFamily is the h1 auth header a family-pinned platform transport
// sends to declare the family it intends to prove ("4" or "6"). Absent on a
// family-agnostic transport. The h3 control stream carries the same intent in
// `protocol.Auth.ip_family`. The server counts the connection as proof only
// when this agrees with the family it observed the connection arrive on.
const HeaderIpFamily = "X-UR-IpFamily"

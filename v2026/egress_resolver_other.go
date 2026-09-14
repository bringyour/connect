//go:build !windows && (!linux || android)

package connect

import "net"

// On these platforms self-exclusion IS handled at the OS layer, so control
// dials keep the platform resolver: nil means net.Dialer uses
// net.DefaultResolver, exactly as before. macOS network extensions route the
// extension's own lookups around their tunnel, and Android VpnService does the
// same for the app's (VpnService.protect / addDisallowedApplication).
//
// The build tag excludes Linux by hand because Go sets the `linux` tag for
// GOOS=android too, and a plain `!linux` would take Android's OS-layer answer
// away from it. Desktop Linux is the platform where the assumption above
// actually fails — systemd-resolved answers the daemon's lookups from its own
// cgroup, unmarked, straight into the tunnel — and it has its own
// implementation in egress_resolver_linux.go. See that file's header for the
// deadlock this partition exists to describe.
func egressResolver() *net.Resolver {
	return nil
}

//go:build !windows && !darwin

package connect

// On other platforms self-exclusion is handled at the OS layer. Android uses
// VpnService.protect / addDisallowedApplication. The forced-egress binding is a
// no-op so the egress control is inert in those builds.
func applyEgressInterface(_ uintptr, _ uint32, _ uint32) error {
	return nil
}

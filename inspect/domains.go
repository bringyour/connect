package main

import (
	"strings"
)

func mergeTimestamps(ts1, ts2 *timestamps) {
	ts1.times = append(ts1.times, ts2.times...)
}

// SplitDomain splits a domain into subDomain, thirdLevelDomain, secondLevelDomain, and topLevelDomain.
func splitDomain(domain string) (subDomain, thirdLevelDomain, secondLevelDomain, topLevelDomain string) {
	parts := strings.Split(domain, ".")

	if len(parts) < 2 {
		// If the domain is something like 'localhost' without a dot, return it as the top-level domain
		return "", "", domain, ""
	}

	// Top-level domain (last part)
	topLevelDomain = "." + parts[len(parts)-1]

	// Second-level domain (second-to-last part and last part)
	secondLevelDomain = strings.Join(parts[len(parts)-2:], ".")

	// Third-level domain (third-to-last part, second-to-last part, and last part)
	if len(parts) > 2 {
		thirdLevelDomain = strings.Join(parts[len(parts)-3:], ".")
	}

	// Subdomain (everything before the third-level domain)
	if len(parts) > 3 {
		subDomain = strings.Join(parts[:len(parts)-3], ".")
	}

	return
}

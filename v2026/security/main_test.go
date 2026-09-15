package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Pins deterministic, payload-free response provenance.
func TestEmitInputProvenance(t *testing.T) {
	const contentSHA256 = "9162a5e95885f667bccc29629214ed38ff9d5bd16c2ad6e5eaea79fc9273eb7b"
	content := []byte("; Last-Modified: 2099-01-02T03:04:05Z\n1.2.3.4\n2001:4860::1\n8.8.8.0/24\nbad\n")
	success := parseResponse(feed{name: "z-success", url: "https://feeds.example/z", kind: kindText, credit: "success credit"}, content)
	success.disposition = feedDispositionAcceptedBlock
	if got := fmt.Sprintf("%x", success.contentSHA256); got != contentSHA256 {
		t.Fatalf("content SHA-256 = %s, want %s", got, contentSHA256)
	}
	if success.contentBytes != 74 {
		t.Fatalf("content bytes = %d, want 74", success.contentBytes)
	}
	skipped := parseResponse(feed{name: "m-skipped", url: "https://feeds.example/m", kind: kindText, credit: "skipped credit"}, []byte("2.2.2.2\n"))
	skipped.disposition = feedDispositionSkippedMinCount

	unavailable := feed{name: "a-unavailable", url: "https://feeds.example/a", credit: "unavailable credit"}
	out1 := string(emit(success.ranges, success.ranges6, "", []result{
		success,
		skipped,
		{f: unavailable, err: fmt.Errorf("volatile failure one")},
	}))
	out2 := string(emit(success.ranges, success.ranges6, "", []result{
		success,
		skipped,
		{f: unavailable, err: fmt.Errorf("different volatile failure")},
	}))
	if out1 != out2 {
		t.Fatal("unavailable response details made emission nondeterministic")
	}
	wantSuccess := "name=z-success url=https://feeds.example/z content_sha256=" + contentSHA256 + " content_bytes=74 parsed_v4_ranges=2 parsed_v6_ranges=1 parsed_hosts=2 parsed_cidrs=1 parsed_bad=1 disposition=accepted_block"
	wantSkipped := "name=m-skipped url=https://feeds.example/m content_sha256=71d34b5c428f6c21fc20cb17ba036a5090081330e57c7580c8090aa66ab91257 content_bytes=8 parsed_v4_ranges=1 parsed_v6_ranges=0 parsed_hosts=1 parsed_cidrs=0 parsed_bad=0 disposition=skipped_below_min_count"
	wantUnavailable := "name=a-unavailable url=https://feeds.example/a disposition=unavailable"
	for _, want := range []string{wantSuccess, wantSkipped, wantUnavailable} {
		if strings.Count(out1, want) != 1 {
			t.Errorf("provenance entry %q count = %d, want 1", want, strings.Count(out1, want))
		}
	}
	if strings.Index(out1, wantSuccess) >= strings.Index(out1, wantSkipped) || strings.Index(out1, wantSkipped) >= strings.Index(out1, wantUnavailable) {
		t.Fatal("provenance entries do not preserve declared feed order")
	}
	for _, forbidden := range []string{"Last-Modified", "2099-01-02", "1.2.3.4", "2.2.2.2", "2001:4860::1", "8.8.8.0/24", "volatile failure", "different volatile failure"} {
		if strings.Contains(out1, forbidden) {
			t.Errorf("generated output leaked %q", forbidden)
		}
	}
}

// Rejects stale or unavailable checked-in inputs.
func TestCheckedInGeneratedInputProvenance(t *testing.T) {
	generatedBytes, err := os.ReadFile(filepath.Join("..", "ip_security_cfaa_block.go"))
	if err != nil {
		t.Fatal(err)
	}
	generated := string(generatedBytes)
	const heading = "// Generated input provenance v1 (declared feed order):"
	if count := strings.Count(generated, heading); count != 1 {
		t.Fatalf("generated provenance heading count = %d, want 1", count)
	}
	const availableFields = ` content_sha256=[0-9a-f]{64} content_bytes=[0-9]+ parsed_v4_ranges=[0-9]+ parsed_v6_ranges=[0-9]+ parsed_hosts=[0-9]+ parsed_cidrs=[0-9]+ parsed_bad=[0-9]+ disposition=accepted_(?:block|deprecated)`
	availableRecord := regexp.MustCompile(`(?m)^//   - name=[^ ]+ url=[^ ]+` + availableFields + `$`)
	if count := len(availableRecord.FindAllString(generated, -1)); count != len(feeds) {
		t.Fatalf("valid available provenance records = %d, want %d", count, len(feeds))
	}
	rejectedRecord := regexp.MustCompile(`(?m)^//   - .* disposition=(?:unavailable|skipped_below_min_count)$`)
	if count := len(rejectedRecord.FindAllString(generated, -1)); count != 0 {
		t.Fatalf("unavailable or skipped provenance records = %d, want 0", count)
	}
	previous := strings.Index(generated, heading)
	for _, f := range feeds {
		disposition := feedDispositionAcceptedBlock
		contentBytesPattern := `[1-9][0-9]*`
		if f.deprecated {
			disposition = feedDispositionAcceptedDeprecated
			contentBytesPattern = `[0-9]+`
		}
		pattern := regexp.MustCompile(`(?m)^//   - name=` + regexp.QuoteMeta(f.name) + ` url=` + regexp.QuoteMeta(f.url) + ` content_sha256=[0-9a-f]{64} content_bytes=` + contentBytesPattern + ` parsed_v4_ranges=([0-9]+) parsed_v6_ranges=([0-9]+) parsed_hosts=[0-9]+ parsed_cidrs=[0-9]+ parsed_bad=[0-9]+ disposition=` + disposition + `$`)
		matches := pattern.FindAllStringSubmatchIndex(generated, -1)
		if len(matches) != 1 {
			t.Errorf("provenance records for %s = %d, want 1 available record", f.name, len(matches))
			continue
		}
		if matches[0][0] <= previous {
			t.Errorf("provenance record for %s is outside declared feed order", f.name)
		}
		v4Count, v4Err := strconv.Atoi(generated[matches[0][2]:matches[0][3]])
		v6Count, v6Err := strconv.Atoi(generated[matches[0][4]:matches[0][5]])
		if v4Err != nil || v6Err != nil {
			t.Errorf("provenance counts for %s: v4 error=%v, v6 error=%v", f.name, v4Err, v6Err)
		} else if !f.deprecated && v4Count+v6Count < f.minCount {
			t.Errorf("provenance entries for %s = %d, want configured minimum %d", f.name, v4Count+v6Count, f.minCount)
		} else if !f.deprecated && v6Count < f.minV6Count {
			t.Errorf("provenance IPv6 entries for %s = %d, want configured minimum %d", f.name, v6Count, f.minV6Count)
		}
		previous = matches[0][0]
	}
}

// Preserves a successfully fetched empty deprecated feed as explicit evidence.
func TestEmptyDeprecatedResponseProvenance(t *testing.T) {
	results := []result{
		parseResponse(feed{name: "empty", url: "https://feeds.example/empty", kind: kindText, deprecated: true}, nil),
	}
	aggregated := aggregateResults(results)
	if len(aggregated.problems) != 0 || len(aggregated.ranges) != 0 || len(aggregated.ranges6) != 0 {
		t.Fatalf("empty deprecated aggregation = problems:%q v4:%d v6:%d, want empty accepted result", aggregated.problems, len(aggregated.ranges), len(aggregated.ranges6))
	}
	if results[0].disposition != feedDispositionAcceptedDeprecated {
		t.Fatalf("disposition = %q, want %q", results[0].disposition, feedDispositionAcceptedDeprecated)
	}
	emitted := string(emit([]iprange{{lo: 1, hi: 1}}, nil, "", results))
	const want = "name=empty url=https://feeds.example/empty content_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 content_bytes=0 parsed_v4_ranges=0 parsed_v6_ranges=0 parsed_hosts=0 parsed_cidrs=0 parsed_bad=0 disposition=accepted_deprecated"
	if strings.Count(emitted, want) != 1 {
		t.Fatalf("empty deprecated provenance count = %d, want 1", strings.Count(emitted, want))
	}
}

// Pins allow-stale filtering and deprecated-feed error behavior.
func TestAggregateResultsExcludesUnusableFeeds(t *testing.T) {
	results := []result{
		parseResponse(feed{name: "accepted", kind: kindText, minCount: 1}, []byte("1.1.1.1\n")),
		parseResponse(feed{name: "below", kind: kindText, minCount: 2}, []byte("2.2.2.2\n")),
		parseResponse(feed{name: "deprecated", kind: kindText, deprecated: true}, []byte("3.3.3.3\n")),
		{f: feed{name: "failed"}, err: fmt.Errorf("fetch failed")},
		{f: feed{name: "deprecated-failed", deprecated: true}, err: fmt.Errorf("deprecated fetch failed")},
	}
	aggregated := aggregateResults(results)
	if len(aggregated.ranges) != 2 || aggregated.ranges[0].lo != 0x01010101 || aggregated.ranges[1].lo != 0x03030303 {
		t.Fatalf("accepted ranges = %#v, want only 1.1.1.1 and deprecated 3.3.3.3", aggregated.ranges)
	}
	if len(aggregated.ranges6) != 0 {
		t.Fatalf("accepted IPv6 ranges = %d, want 0", len(aggregated.ranges6))
	}
	if len(aggregated.problems) != 3 || !strings.HasPrefix(aggregated.problems[0], "below:") || !strings.HasPrefix(aggregated.problems[1], "failed:") || !strings.HasPrefix(aggregated.problems[2], "deprecated-failed:") {
		t.Fatalf("problems = %q, want below-min and both fetch failures", aggregated.problems)
	}
	wantDispositions := []string{
		feedDispositionAcceptedBlock,
		feedDispositionSkippedMinCount,
		feedDispositionAcceptedDeprecated,
		feedDispositionUnavailable,
		feedDispositionUnavailable,
	}
	for i, want := range wantDispositions {
		if results[i].disposition != want {
			t.Errorf("result %d (%s) disposition = %q, want %q", i, results[i].f.name, results[i].disposition, want)
		}
	}
}

// Rejects wrong-family content from a feed with an IPv6 floor.
func TestAggregateResultsRejectsBelowMinimumIPv6Feed(t *testing.T) {
	results := []result{
		parseResponse(feed{name: "v6", kind: kindText, minCount: 1, minV6Count: 1}, []byte("1.2.3.4\n")),
	}
	aggregated := aggregateResults(results)
	if len(aggregated.ranges) != 0 || len(aggregated.ranges6) != 0 {
		t.Fatalf("accepted ranges = v4:%d v6:%d, want none", len(aggregated.ranges), len(aggregated.ranges6))
	}
	if len(aggregated.problems) != 1 || !strings.HasPrefix(aggregated.problems[0], "v6:") || !strings.Contains(aggregated.problems[0], "IPv6") {
		t.Fatalf("problems = %q, want one IPv6 feed floor failure", aggregated.problems)
	}
	if results[0].disposition != feedDispositionSkippedMinCount {
		t.Fatalf("disposition = %q, want %q", results[0].disposition, feedDispositionSkippedMinCount)
	}
}

// Covers both generated policy floors.
func TestValidateMinimumRanges(t *testing.T) {
	for _, test := range []struct {
		name    string
		v4Count int
		v6Count int
		want    string
	}{
		{name: "exact floors", v4Count: 10_000, v6Count: 100},
		{name: "IPv4 below floor", v4Count: 9_999, v6Count: 100, want: "IPv4"},
		{name: "IPv6 below floor", v4Count: 10_000, v6Count: 99, want: "IPv6"},
	} {
		err := validateMinimumRanges(test.v4Count, test.v6Count, 10_000, 100)
		if test.want == "" {
			if err != nil {
				t.Errorf("%s: unexpected error: %v", test.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: error = %v, want one containing %q", test.name, err, test.want)
		}
	}
}

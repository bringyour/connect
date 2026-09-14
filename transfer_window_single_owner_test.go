package connect

import (
	"os"
	"strings"
	"testing"
)

// The structural property behind three separate defects in this program: a
// value computed correctly and then not consumed.
//
// The ceiling was derived and then overwritten by the initial size. The
// interval was computed from a resend timer rather than from the path. The
// delivery estimate was summed over the wrong horizon. Each was one place
// reading an ingredient of the window instead of the window, and each was found
// by measurement after it had already cost time. A test that makes a fourth
// impossible is worth more than three tests that catch the three we found.
//
// So this asserts ownership rather than behaviour: the window has exactly one
// place that computes it, and the ingredients that go into it are read nowhere
// else. A new consumer that reaches for the share, the peer's advertised
// capacity, the configured ceiling, the target or the delivery rate — rather
// than for the window the estimator returns — fails here, at the point it is
// written, instead of in a campaign three weeks later.
//
// This is a source-level assertion, which is unusual and deliberate. The
// property is about which code may read what, and no runtime test can express
// that: every runtime test can only observe the consequences of a wrong reader
// after someone has added one.
//
// Derived rather than counted, and named as such: the enclosing function of an
// occurrence is derived by scanning back to the nearest line beginning "func ",
// which is exact for this file's formatting (gofmt puts every declaration at
// column zero) and would need a real parser if that changed.
func TestTheWindowHasOneOwner(t *testing.T) {
	source, err := os.ReadFile("transfer.go")
	if err != nil {
		t.Fatalf("read transfer.go: %v", err)
	}
	lines := strings.Split(string(source), "\n")

	// the function each line belongs to, derived by scanning back to the
	// nearest declaration at column zero
	enclosing := make([]string, len(lines))
	current := ""
	for i, line := range lines {
		// a closing brace at column zero ends the declaration, so a type
		// declared after a function is not counted as inside it
		if line == "}" {
			current = ""
		}
		if strings.HasPrefix(line, "func ") {
			name := line[len("func "):]
			if strings.HasPrefix(name, "(") {
				if close := strings.Index(name, ") "); 0 <= close {
					name = name[close+2:]
				}
			}
			if open := strings.Index(name, "("); 0 <= open {
				name = name[:open]
			}
			current = strings.TrimSpace(name)
		}
		enclosing[i] = current
	}

	// The ingredients of the window, and the only functions allowed to read
	// each. Everything else must read the estimate's Window.
	owners := map[string][]string{
		"DeliverySizedWindowCeilingByteCount": {
			"ApplyWindowSizing",
			"DefaultSendBufferSettingsWithBufferSize",
			"sendWindowEstimate",
		},
		"TargetGoodputByteRate": {
			"ApplyWindowSizing",
			"DefaultSendBufferSettingsWithBufferSize",
			"sendWindowEstimate",
			"bindingTerm",
		},
		"LendableByteCount": {
			"sendWindowEstimate",
			"LendableByteCount",
		},
		"receivedWindowAdvertisement": {
			"sendWindowEstimate",
			"bindingTerm",
			"receivedWindowAdvertisement",
		},
		"deliveredRate": {
			"sendWindowEstimate",
			"deliveredRate",
		},
	}

	for symbol, allowed := range owners {
		allowedSet := map[string]bool{}
		for _, name := range allowed {
			allowedSet[name] = true
		}
		found := 0
		for i, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") || !strings.Contains(line, symbol) {
				continue
			}
			// a field or constant declared outside any function is the
			// definition rather than a reader of it
			if enclosing[i] == "" {
				continue
			}
			found += 1
			if !allowedSet[enclosing[i]] {
				t.Errorf(
					"transfer.go:%d reads %s inside %s, which is not one of the window's owners (%s); a consumer that reaches for an ingredient rather than for the window the estimator returns is the shape of the three defects this program has already paid for",
					i+1, symbol, enclosing[i], strings.Join(allowed, ", "),
				)
			}
		}
		if found == 0 {
			t.Errorf(
				"%s was not found at all, so this row is guarding a name that no longer exists and is asserting nothing",
				symbol,
			)
		}
	}

	// and the positive half: the estimator is reached by the two consumers
	// that need a window, admission and the stats snapshot
	consumers := map[string]bool{}
	for i, line := range lines {
		if strings.Contains(line, "sendWindowEstimate(") &&
			!strings.HasPrefix(strings.TrimSpace(line), "//") &&
			enclosing[i] != "sendWindowEstimate" {
			consumers[enclosing[i]] = true
		}
	}
	t.Logf("window consumers: %v", consumers)
	if len(consumers) < 2 {
		t.Errorf(
			"only %d function reads the window estimate; admission and the stats snapshot both need it, so a count below two means one of them found another way to a number",
			len(consumers),
		)
	}
}

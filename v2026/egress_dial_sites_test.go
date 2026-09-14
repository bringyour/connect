package connect

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every control dial in this package must reach the network through the
// egress-aware machinery (ConnectSettings.NetDialer / DialContext /
// resolveEgressUDPAddr), or its socket/name resolution falls into the tunnel
// this process provides on Windows (the connect-hang root cause). This test
// pins that statically: raw net dials, raw OS-resolver lookups, and the
// default http client/transport are forbidden in package-root non-test files,
// except at the sites audited below.
//
// If this test fails on new code, dial through ConnectSettings (NetDialer or
// DialContext) or resolveEgressUDPAddr instead — or, if the new site really
// is exempt (a connect()-only local-address probe, the egress machinery
// itself), add it to the allowlist WITH the reason.
var allowedRawDialSites = map[string]map[string]string{
	"ice_net.go": {
		"net.Dial": "connect()-only UDP local-address probe; no packet is sent",
	},
	"egress_dial.go": {
		"net.DefaultResolver": "the context-aware no-egress path inside resolveEgressUDPAddr itself, " +
			"resolving to a list so pickControlIPAddr can honor the family policy; and dialResolver, " +
			"the same fallback for the hostname dial race in ConnectSettings.DialContext",
	},
	"ice_resolve_net.go": {
		"net.DefaultResolver": "the lifecycle-aware fallback inside the per-peer egress resolver",
	},
}

var forbiddenNetCalls = map[string]bool{
	"Dial":            true,
	"DialTimeout":     true,
	"DialUDP":         true,
	"DialTCP":         true,
	"DialIP":          true,
	"ResolveUDPAddr":  true,
	"ResolveTCPAddr":  true,
	"ResolveIPAddr":   true,
	"LookupIP":        true,
	"LookupHost":      true,
	"LookupIPAddr":    true,
	"DefaultResolver": true,
}

var forbiddenHttpIdents = map[string]bool{
	"DefaultClient":    true,
	"DefaultTransport": true,
}

func TestControlDialSitesUseTheBoundDialer(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var violations []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		allowed := allowedRawDialSites[name]
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			var call string
			switch {
			case pkg.Name == "net" && forbiddenNetCalls[sel.Sel.Name]:
				call = "net." + sel.Sel.Name
			case pkg.Name == "http" && forbiddenHttpIdents[sel.Sel.Name]:
				call = "http." + sel.Sel.Name
			default:
				return true
			}
			if _, ok := allowed[call]; ok {
				return true
			}
			pos := fset.Position(sel.Pos())
			violations = append(violations, fmt.Sprintf("%s:%d: %s bypasses the egress-aware dial path", pos.Filename, pos.Line, call))
			return true
		})
	}
	if 0 < len(violations) {
		t.Fatalf("raw dial/resolve sites outside the audited allowlist:\n%s", strings.Join(violations, "\n"))
	}
}

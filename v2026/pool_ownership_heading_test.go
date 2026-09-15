package connect

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// THROUGHPUT-TESTGAPS U-13, THROUGHPUTFIX §29.3. Two new cells in one session
// produced two pool ownership violations, and the reason both were easy to
// write is that adjacent entry points a layer apart apply different rules and
// nothing in their names says which. A sequence-level entry TAKES the buffer
// and returns it after its callback; the callback-level entry one layer above
// BORROWS it and returns nothing; a send entry TAKES IT ON SUCCESS, so the
// caller must read the result before deciding who returns. §29.3's remedy is
// that every entry point states one of the three words in its own doc comment
// and CODESTYLE lists them under those headings, "so the rule is looked up
// rather than inferred".
//
// CODESTYLE's list landed. The doc comments did not: eight of the fourteen
// entry points it names carried no word, including both batch entries, whose
// `true` return reads as an ownership transfer and means only delivered. That
// is the state this row failed against before the headings were written, and
// the reason the row exists rather than a re-reading of the convention: a
// convention that lives only in a document is checked by whoever remembers to
// read it, and a new entry point added beside one of these is exactly the case
// where nobody does.
//
// What it asserts, and why in this shape. Every entry point CODESTYLE names
// must exist under that name and must say its own word, and the three words
// must remain distinguishable — a "takes" that does not say "on success" is a
// different contract from one that does, and confusing the two is the
// over-return §29.3 caught. The row reads the package's own source rather than
// a table of strings kept beside it, so a rename, a removal, or a new entry
// point that inherits a doc comment from the wrong neighbour is a failure
// here.
//
// What it cannot do: check that the declaration matches the behaviour. That
// half of U-13's contract belongs to the pool boundary reconciliation and to
// the fixtures that run it. This row holds the half that is decidable from the
// source, which is the half that was actually missing.
func TestEveryPoolBufferEntryPointDeclaresItsOwnership(t *testing.T) {
	const (
		borrows        = "borrows"
		takes          = "takes"
		takesOnSuccess = "takes on success"
	)

	// The list is CODESTYLE's "Which entry points borrow, take, or take on
	// success", transcribed. A change to that section and a change here are
	// one edit.
	entries := []struct {
		name string
		rule string
	}{
		{"RemoteUserNatProvider.Receive", borrows},
		{"RemoteUserNatProvider.receiveTransfer", borrows},
		{"RemoteUserNatProvider.receiveTransferWithRecovery", borrows},
		{"RemoteUserNatProvider.ReceiveBatch", borrows},
		{"RemoteUserNatProvider.receiveTransferBatch", borrows},
		{"ReceiveFunction", borrows},
		{"ReceivePacketFunction", borrows},
		{"TcpSequence.receivePacket", takes},
		{"TcpSequence.receiveBatch", takes},
		{"LocalUserNat.SendPacket", takesOnSuccess},
		{"LocalUserNat.SendPacketWithTimeout", takesOnSuccess},
		{"LocalUserNat.SendPackets", takesOnSuccess},
		{"Client.SendWithTimeout", takesOnSuccess},
		{"Client.SendWithTimeoutDetailed", takesOnSuccess},
	}

	docs := packageDocComments(t)

	for _, entry := range entries {
		doc, ok := docs[entry.name]
		if !ok {
			t.Errorf(
				"%s is listed in CODESTYLE's pool ownership headings and no declaration of that name is in the package. A renamed or removed entry point leaves the heading pointing at nothing, which is the state the convention was written to end",
				entry.name,
			)
			continue
		}
		lowered := strings.ToLower(doc)
		saysBorrows := strings.Contains(lowered, borrows) ||
			strings.Contains(lowered, "borrowed")
		saysTakes := strings.Contains(lowered, takes)
		saysOnSuccess := strings.Contains(lowered, "on success")

		switch entry.rule {
		case borrows:
			if !saysBorrows {
				t.Errorf(
					"%s borrows its buffer and its doc comment does not say so. A caller that built the buffer must return it after the call; without the word, the adjacent taking entry one layer away is the natural reading and the under-return is the result (THROUGHPUTFIX §29.3). Doc comment: %q",
					entry.name,
					doc,
				)
			}
		case takes:
			if !saysTakes {
				t.Errorf(
					"%s takes its buffer and its doc comment does not say so. The callee returns it, so a caller that returns it as well over-returns a buffer no owner held. Doc comment: %q",
					entry.name,
					doc,
				)
			}
			if saysOnSuccess {
				t.Errorf(
					"%s takes its buffer unconditionally, and its doc comment says \"on success\". The two contracts differ in who owns the buffer after a false return, and reading one as the other is the violation the convention exists to prevent. Doc comment: %q",
					entry.name,
					doc,
				)
			}
		case takesOnSuccess:
			if !saysTakes || !saysOnSuccess {
				t.Errorf(
					"%s takes its buffer on success only and its doc comment does not say so in those words. A true return transfers ownership and a false return leaves it with the caller; a batch entry's true return reads as a transfer and means delivered, which is the reading that compounds the error. Doc comment: %q",
					entry.name,
					doc,
				)
			}
		}
	}
}

// Every declaration in this package's non-test sources, keyed as CODESTYLE
// names them: `Type.Method` for a method, the bare name for a function or a
// type. Parsed from source rather than reflected, because a doc comment is not
// in the binary and the doc comment is the thing under test.
//
// `parseProductionGo` in `receive_callback_policy_test.go` is the neighbouring
// audit's walk and is not reused: it parses with mode 0, which discards
// comments, and it walks every package in the repository, which would let two
// packages' identically named declarations collide in one key. This reads the
// one package directory with comments kept.
func packageDocComments(t *testing.T) map[string]string {
	t.Helper()

	dirEntries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %s", err)
	}

	fileSet := token.NewFileSet()
	docs := map[string]string{}
	for _, dirEntry := range dirEntries {
		name := dirEntry.Name()
		// every source in the directory, build tags included: a
		// platform-specific entry point is under the same convention as a
		// portable one
		if dirEntry.IsDir() ||
			!strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, name, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parsing %s: %s", name, err)
		}
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				declName := decl.Name.Name
				if decl.Recv != nil && len(decl.Recv.List) == 1 {
					declName = receiverTypeName(decl.Recv.List[0].Type) + "." + declName
				}
				docs[declName] = decl.Doc.Text()
			case *ast.GenDecl:
				if decl.Tok != token.TYPE {
					continue
				}
				for _, spec := range decl.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					doc := typeSpec.Doc.Text()
					if doc == "" {
						// a single-spec declaration carries its doc on the
						// declaration rather than on the spec
						doc = decl.Doc.Text()
					}
					docs[typeSpec.Name.Name] = doc
				}
			}
		}
	}
	return docs
}

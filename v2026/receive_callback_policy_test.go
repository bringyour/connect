// Receive-callback policy tests inventory every production Client subscriber
// and reject obvious blocking work at those shared delivery boundaries.
package connect

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// receiveCallbackRegistration identifies one production callback subscription
// without depending on its line number.
type receiveCallbackRegistration struct {
	path        string
	callback    string
	funcLitBody *ast.BlockStmt
}

// receiveCallbackBody identifies the method behind an audited method value.
type receiveCallbackBody struct {
	path     string
	receiver string
	method   string
}

// parsedProductionGo contains one source snapshot and its shared position set.
type parsedProductionGo struct {
	fileSet *token.FileSet
	files   map[string]*ast.File
}

// parseProductionGo reads every non-test Go source in this repository. A new
// package cannot hide another Client receive subscriber from the audit.
func parseProductionGo(t *testing.T) *parsedProductionGo {
	t.Helper()
	fileSet := token.NewFileSet()
	files := map[string]*ast.File{}
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != "." && strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		path = filepath.ToSlash(path)
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		files[path] = file
		return nil
	})
	if err != nil {
		t.Fatalf("parse production Go: %v", err)
	}
	return &parsedProductionGo{fileSet: fileSet, files: files}
}

// formatAstExpr returns the stable source spelling used by the inventory.
func formatAstExpr(t *testing.T, fileSet *token.FileSet, expression ast.Expr) string {
	t.Helper()
	if _, ok := expression.(*ast.FuncLit); ok {
		return "func literal"
	}
	var buffer bytes.Buffer
	if err := format.Node(&buffer, fileSet, expression); err != nil {
		t.Fatalf("format callback expression: %v", err)
	}
	return buffer.String()
}

// productionClientCallbackRegistrations returns every receive/forward
// subscription and retains anonymous bodies for the direct-call audit.
func productionClientCallbackRegistrations(
	t *testing.T,
	parsed *parsedProductionGo,
) (receives []receiveCallbackRegistration, forwards []receiveCallbackRegistration) {
	t.Helper()
	for path, file := range parsed.files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			if selector.Sel.Name != "AddReceiveCallback" && selector.Sel.Name != "AddForwardCallback" {
				return true
			}
			registration := receiveCallbackRegistration{
				path:     path,
				callback: formatAstExpr(t, parsed.fileSet, call.Args[0]),
			}
			if function, ok := call.Args[0].(*ast.FuncLit); ok {
				registration.funcLitBody = function.Body
			}
			if selector.Sel.Name == "AddReceiveCallback" {
				receives = append(receives, registration)
			} else {
				forwards = append(forwards, registration)
			}
			return true
		})
	}
	sort.Slice(receives, func(i int, j int) bool {
		if receives[i].path != receives[j].path {
			return receives[i].path < receives[j].path
		}
		return receives[i].callback < receives[j].callback
	})
	sort.Slice(forwards, func(i int, j int) bool {
		if forwards[i].path != forwards[j].path {
			return forwards[i].path < forwards[j].path
		}
		return forwards[i].callback < forwards[j].callback
	})
	return
}

// receiverTypeName unwraps the pointer receiver used by this codebase.
func receiverTypeName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.StarExpr:
		return receiverTypeName(value.X)
	default:
		return ""
	}
}

// findReceiveCallbackBody resolves one method from the explicit inventory.
func findReceiveCallbackBody(
	t *testing.T,
	parsed *parsedProductionGo,
	identity receiveCallbackBody,
) *ast.BlockStmt {
	t.Helper()
	file := parsed.files[identity.path]
	if file == nil {
		t.Fatalf("audited callback source %s is missing", identity.path)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != identity.method || function.Recv == nil || len(function.Recv.List) != 1 {
			continue
		}
		if receiverTypeName(function.Recv.List[0].Type) == identity.receiver {
			return function.Body
		}
	}
	t.Fatalf("audited callback %s.%s is missing from %s", identity.receiver, identity.method, identity.path)
	return nil
}

// zeroInteger reports the exact zero-timeout spelling required at a receive
// handoff; an implicit/default timeout is not accepted by this guard.
func zeroInteger(expression ast.Expr) bool {
	literal, ok := expression.(*ast.BasicLit)
	return ok && literal.Kind == token.INT && literal.Value == "0"
}

// callName returns an unqualified function or method name.
func callName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return value.Sel.Name
	default:
		return ""
	}
}

// assertReceiveCallbackBodyNonblocking rejects direct waits, blocking sender
// APIs, nonzero-timeout handoffs, and channel sends without a default case.
// The explicit callback inventory remains the call-graph audit for indirect
// helpers; this structural layer catches the easy regression at review time.
func assertReceiveCallbackBodyNonblocking(
	t *testing.T,
	parsed *parsedProductionGo,
	label string,
	body *ast.BlockStmt,
) {
	t.Helper()
	nonblockingSends := map[*ast.SendStmt]bool{}
	ast.Inspect(body, func(node ast.Node) bool {
		selection, ok := node.(*ast.SelectStmt)
		if !ok {
			return true
		}
		hasDefault := false
		for _, statement := range selection.Body.List {
			clause := statement.(*ast.CommClause)
			if clause.Comm == nil {
				hasDefault = true
				break
			}
		}
		if hasDefault {
			for _, statement := range selection.Body.List {
				clause := statement.(*ast.CommClause)
				if send, ok := clause.Comm.(*ast.SendStmt); ok {
					nonblockingSends[send] = true
				}
			}
		}
		return true
	})

	blockingCalls := map[string]bool{
		"CloseAndWait": true,
		"Forward":      true,
		"Send":         true,
		"SendControl":  true,
		"SendMultiHop": true,
		"Sleep":        true,
		"Wait":         true,
		"WaitForExit":  true,
	}
	timeoutArguments := map[string]int{
		"Ack":                            2,
		"ForwardWithTimeout":             1,
		"ForwardWithTimeoutDetailed":     1,
		"Pack":                           1,
		"SendControlWithTimeout":         2,
		"SendMultiHopWithTimeout":        3,
		"SendPacket":                     3,
		"SendPacketBatch":                3,
		"SendPackets":                    3,
		"SendWithTimeout":                3,
		"sendTransferPacketsWithTimeout": 4,
	}

	ast.Inspect(body, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.SendStmt:
			if !nonblockingSends[value] {
				position := parsed.fileSet.Position(value.Pos())
				t.Errorf("%s has a blocking channel handoff at %s", label, position)
			}
		case *ast.CallExpr:
			name := callName(value.Fun)
			if blockingCalls[name] {
				position := parsed.fileSet.Position(value.Pos())
				t.Errorf("%s calls blocking %s at %s", label, name, position)
				return true
			}
			argumentIndex, checked := timeoutArguments[name]
			if !checked {
				return true
			}
			if len(value.Args) <= argumentIndex || !zeroInteger(value.Args[argumentIndex]) {
				position := parsed.fileSet.Position(value.Pos())
				t.Errorf("%s calls %s without literal timeout 0 at %s", label, name, position)
			}
		}
		return true
	})
}

// TestProductionClientReceiveCallbacksAreAudited makes a new subscriber fail
// until its ownership and congestion behavior are explicitly reviewed. No
// production forward subscriber exists; adding one requires the same audit.
func TestProductionClientReceiveCallbacksAreAudited(t *testing.T) {
	parsed := parseProductionGo(t)
	receives, forwards := productionClientCallbackRegistrations(t, parsed)
	expected := []receiveCallbackRegistration{
		{path: "connectctl/main.go", callback: "func literal"},
		{path: "ip.go", callback: "userNatClient.ClientReceive"},
		{path: "ip.go", callback: "userNatProvider.ClientReceive"},
		{path: "ip_remote_multi_client.go", callback: "clientChannel.clientReceive"},
		{path: "transfer.go", callback: "peerManager.Receive"},
		{path: "transfer.go", callback: "streamManager.Receive"},
		{path: "transport_p2p_webrtc.go", callback: "dispatcher.Receive"},
	}
	if len(receives) != len(expected) {
		t.Fatalf("production receive registrations = %v, want %v; audit the new boundary", receives, expected)
	}
	for index := range expected {
		if receives[index].path != expected[index].path || receives[index].callback != expected[index].callback {
			t.Fatalf("production receive registrations = %v, want %v; audit the changed boundary", receives, expected)
		}
	}
	if len(forwards) != 0 {
		t.Fatalf("production forward registrations = %v; audit the new boundary", forwards)
	}

	methodBodies := []receiveCallbackBody{
		{path: "ip.go", receiver: "RemoteUserNatClient", method: "ClientReceive"},
		{path: "ip.go", receiver: "RemoteUserNatProvider", method: "ClientReceive"},
		{path: "ip_remote_multi_client.go", receiver: "multiClientChannel", method: "clientReceive"},
		{path: "transfer_peer_manager.go", receiver: "PeerManager", method: "Receive"},
		{path: "transfer_stream_manager.go", receiver: "StreamManager", method: "Receive"},
		{path: "transport_p2p_webrtc.go", receiver: "clientSignalDispatcher", method: "Receive"},
	}
	for _, identity := range methodBodies {
		label := identity.receiver + "." + identity.method
		assertReceiveCallbackBodyNonblocking(
			t,
			parsed,
			label,
			findReceiveCallbackBody(t, parsed, identity),
		)
	}
	for _, registration := range receives {
		if registration.funcLitBody != nil {
			assertReceiveCallbackBodyNonblocking(
				t,
				parsed,
				registration.path+":"+registration.callback,
				registration.funcLitBody,
			)
		}
	}
}

// TestSharedClientReceivePumpHandoffsUseCarrierPolicy pins the two admissions
// that run before per-sequence callbacks. Pack may use only the bounded
// exact carrier-lane handoff policy. Neither may inherit
// ClientSettings.BufferTimeout; reliable lanes wait and datagrams remain
// zero-wait independently of the broad transport family.
func TestSharedClientReceivePumpHandoffsUseCarrierPolicy(t *testing.T) {
	parsed := parseProductionGo(t)
	body := findReceiveCallbackBody(t, parsed, receiveCallbackBody{
		path:     "transfer.go",
		receiver: "Client",
		method:   "run",
	})
	counts := map[string]int{}
	packTimeoutPolicyCount := 0
	ackTimeoutPolicyCount := 0
	ast.Inspect(body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if ok && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 &&
			formatAstExpr(t, parsed.fileSet, assignment.Lhs[0]) == "handoffTimeout" &&
			strings.Join(strings.Fields(formatAstExpr(t, parsed.fileSet, assignment.Rhs[0])), "") ==
				"self.settings.ReceiveBufferSettings.packHandoffTimeout(transportType,carrierReliability)" {
			packTimeoutPolicyCount++
		}
		if ok && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 &&
			formatAstExpr(t, parsed.fileSet, assignment.Lhs[0]) == "ackHandoffTimeout" &&
			strings.Join(strings.Fields(formatAstExpr(t, parsed.fileSet, assignment.Rhs[0])), "") ==
				"self.settings.ReceiveBufferSettings.ackHandoffTimeout(transportType)" {
			ackTimeoutPolicyCount++
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := formatAstExpr(t, parsed.fileSet, call.Fun)
		argumentIndex := -1
		switch name {
		case "self.receiveBuffer.Pack":
			argumentIndex = 1
		case "self.sendBuffer.ackMessageDetailed":
			argumentIndex = 2
		default:
			return true
		}
		counts[name] += 1
		if len(call.Args) <= argumentIndex {
			position := parsed.fileSet.Position(call.Pos())
			t.Errorf("%s is missing timeout argument at %s", name, position)
			return true
		}
		timeout := formatAstExpr(t, parsed.fileSet, call.Args[argumentIndex])
		if name == "self.receiveBuffer.Pack" && timeout != "handoffTimeout" {
			position := parsed.fileSet.Position(call.Pos())
			t.Errorf("%s timeout = %s at %s, want carrier handoffTimeout", name, timeout, position)
		}
		if name == "self.sendBuffer.ackMessageDetailed" && timeout != "ackHandoffTimeout" {
			position := parsed.fileSet.Position(call.Pos())
			t.Errorf("%s timeout = %s at %s, want carrier ackHandoffTimeout", name, timeout, position)
		}
		return true
	})
	if packTimeoutPolicyCount != 1 {
		t.Errorf("carrier Pack timeout policy assignment count = %d, want 1", packTimeoutPolicyCount)
	}
	if ackTimeoutPolicyCount != 1 {
		t.Errorf("carrier ACK timeout policy assignment count = %d, want 1", ackTimeoutPolicyCount)
	}
	for _, name := range []string{"self.receiveBuffer.Pack", "self.sendBuffer.ackMessageDetailed"} {
		if counts[name] != 1 {
			t.Errorf("%s call count = %d, want 1", name, counts[name])
		}
	}
}

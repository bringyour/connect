//go:build !js

package connect

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Tests are external owners of every manager they create. Close only requests
// teardown so callbacks can safely invoke it; a test cleanup must additionally
// join the admitted Pion, signaling, and peer workers before the next root is
// allowed to observe process state.
func closeTestWebRtcManager(t testing.TB, manager *WebRtcManager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := manager.closeAndWait(ctx); err != nil {
		t.Errorf("join test WebRTC manager: %v", err)
	}
}

// Registers the join at construction so an early Fatal, timeout, or partial
// setup cannot bypass the owner boundary. Individual tests may still exercise
// Close or closeAndWait directly; the final cleanup is idempotent.
func newTestWebRtcManager(
	t testing.TB,
	ctx context.Context,
	signalSender SignalSender,
	settings *WebRtcSettings,
) *WebRtcManager {
	t.Helper()
	manager := NewWebRtcManager(ctx, signalSender, settings)
	t.Cleanup(func() {
		closeTestWebRtcManager(t, manager)
	})
	return manager
}

// Every test and benchmark manager must be registered with the joining owner
// at construction. A deferred Close is insufficient because it only requests
// teardown and lets the next root race Pion cleanup.
func TestWebRtcTestManagersHaveJoiningOwners(t *testing.T) {
	_, helperPath, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate WebRTC test owner helper")
	}
	directory := filepath.Dir(helperPath)
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	rawConstructorCount := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if parseErr != nil {
			t.Errorf("parse %s: %v", entry.Name(), parseErr)
			continue
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall {
				return true
			}
			constructor, isIdentifier := call.Fun.(*ast.Ident)
			if !isIdentifier || constructor.Name != "NewWebRtcManager" {
				return true
			}
			rawConstructorCount++
			if path != helperPath {
				t.Errorf("%s constructs a test WebRTC manager without a joining owner", entry.Name())
			}
			return true
		})
	}
	if rawConstructorCount != 1 {
		t.Fatalf("raw test WebRTC manager constructors = %d, want only the joining helper", rawConstructorCount)
	}
}

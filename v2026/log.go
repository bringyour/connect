package connect

import (
	"sync/atomic"

	"github.com/urnetwork/glog/v2026"
)

// Logging level convention in the `connect` package and generally for BringYour network components:
// Info:
//     essential events for abnormal behavior. This level should be silent on normal operation,
//     with the exception of infrequent data that is useful for monitoring flows:
//     - backpressure and connectivity timeouts
//     - recoverable abnormal exits.
//       This includes exits caused by external behavior such as bad messages.
//     [glog V(1)]
//     key events for trace debuggung and statistics:
//     - start/end traces
//     - key system events with ids that can be used to trace flows
//     [glog V(2)]
//     specific use-case logging
// Warning:
//     recovered unexpected crash details
// Error:
//     unexpected crash details

// Log messages should be concise and start with a unique [component] tag
// where component is the relevant part of the system.

// `connect` is meant to be embedded, so components log through a `Logger`
// instance instead of the global glog functions. Each top-level settings
// struct has a `Log` field; nil resolves to `DefaultLogger()` at
// construction, and parents pass their logger down to the components they
// create. The glog-backed logger is the default, preserving the historical
// behavior. Embedders that need silence (e.g. a host running thousands of
// clients in one process) set `NewNoopLogger()` on the settings of the
// components they create, and `SetDefaultLogger` for the process-global
// components (message pool, trace).
//
// Outside of this file, glog must not be referenced directly.
type Logger interface {
	Info(args ...any)
	Infof(format string, args ...any)
	Warningf(format string, args ...any)
	Errorf(format string, args ...any)
	V(level int32) Verbose
}

// Verbose mirrors `glog.V` for level-guarded logging,
// so that disabled levels skip argument formatting.
// `if glog.V(2) {` becomes `if log.V(2).Enabled() {`.
type Verbose interface {
	Enabled() bool
	Info(args ...any)
	Infof(format string, args ...any)
}

var defaultLogger atomic.Pointer[Logger]

// DefaultLogger is the logger used by process-global components and wherever
// a settings `Log` is nil. The glog-backed logger unless `SetDefaultLogger`.
func DefaultLogger() Logger {
	if log := defaultLogger.Load(); log != nil {
		return *log
	}
	return glogLogger{}
}

// SetDefaultLogger replaces the package default logger. nil resets to the
// glog-backed logger. Components resolve their logger at construction, so
// set this before creating components.
func SetDefaultLogger(log Logger) {
	if log == nil {
		defaultLogger.Store(nil)
	} else {
		defaultLogger.Store(&log)
	}
}

func loggerOrDefault(log Logger) Logger {
	if log == nil {
		return DefaultLogger()
	}
	return log
}

// NewGlogLogger returns the `Logger` backed by the global glog package.
func NewGlogLogger() Logger {
	return glogLogger{}
}

// the depth variants keep the logged file:line at the caller
type glogLogger struct{}

func (self glogLogger) Info(args ...any) {
	glog.InfoDepth(1, args...)
}

func (self glogLogger) Infof(format string, args ...any) {
	glog.InfoDepthf(1, format, args...)
}

func (self glogLogger) Warningf(format string, args ...any) {
	glog.WarningDepthf(1, format, args...)
}

func (self glogLogger) Errorf(format string, args ...any) {
	glog.ErrorDepthf(1, format, args...)
}

func (self glogLogger) V(level int32) Verbose {
	return glogVerbose(glog.VDepth(1, glog.Level(level)))
}

type glogVerbose glog.Verbose

func (self glogVerbose) Enabled() bool {
	return bool(self)
}

func (self glogVerbose) Info(args ...any) {
	glog.Verbose(self).InfoDepth(1, args...)
}

func (self glogVerbose) Infof(format string, args ...any) {
	glog.Verbose(self).InfoDepthf(1, format, args...)
}

// NewNoopLogger returns a `Logger` that discards everything.
func NewNoopLogger() Logger {
	return noopLogger{}
}

type noopLogger struct{}

func (self noopLogger) Info(args ...any) {
}

func (self noopLogger) Infof(format string, args ...any) {
}

func (self noopLogger) Warningf(format string, args ...any) {
}

func (self noopLogger) Errorf(format string, args ...any) {
}

func (self noopLogger) V(level int32) Verbose {
	return noopVerbose{}
}

type noopVerbose struct{}

func (self noopVerbose) Enabled() bool {
	return false
}

func (self noopVerbose) Info(args ...any) {
}

func (self noopVerbose) Infof(format string, args ...any) {
}

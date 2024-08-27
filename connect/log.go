package connect

import (

// "github.com/golang/glog"
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

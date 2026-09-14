//go:build android

package connect

import (
	"github.com/wlynxg/anet"
)

func init() {
	// see https://github.com/pion/webrtc/issues/2640
	anet.SetAndroidVersion(24)
}

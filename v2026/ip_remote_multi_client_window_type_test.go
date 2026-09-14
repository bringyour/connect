package connect

import (
	"context"
	"net"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestPerformanceProfileFixedWindow pins the auto semantics: a nil profile,
// the zero-value profile, and `WindowTypeAuto` all mean the same "auto" mode —
// no fixed window, and the profile `WindowSize` is ignored.
func TestPerformanceProfileFixedWindow(t *testing.T) {
	var nilProfile *PerformanceProfile
	_, _, ok := nilProfile.FixedWindow()
	AssertEqual(t, false, ok)

	// the zero value is auto
	_, _, ok = (&PerformanceProfile{}).FixedWindow()
	AssertEqual(t, false, ok)

	// an auto profile carries the orthogonal settings but fixes no window,
	// even with a window size set
	autoProfile := &PerformanceProfile{
		WindowType:  WindowTypeAuto,
		WindowSize:  DefaultWindowSizeSettings(),
		AllowDirect: true,
	}
	_, _, ok = autoProfile.FixedWindow()
	AssertEqual(t, false, ok)

	fixedProfile := &PerformanceProfile{
		WindowType: WindowTypeSpeed,
		WindowSize: DefaultWindowSizeSettings(),
	}
	windowType, windowSize, ok := fixedProfile.FixedWindow()
	AssertEqual(t, true, ok)
	AssertEqual(t, WindowTypeSpeed, windowType)
	AssertEqual(t, DefaultWindowSizeSettings(), windowSize)
}

// TestMultiClientSelectWindowTypesAuto verifies a nil profile and an auto
// profile select the same windows (both types, quality first for web), and a
// fixed profile selects only its window type.
func TestMultiClientSelectWindowTypesAuto(t *testing.T) {
	webPacket := &parsedPacket{ipPath: &IpPath{DestinationPort: 443}}
	otherPacket := &parsedPacket{ipPath: &IpPath{DestinationPort: 5000}}

	newMultiClient := func(performanceProfile *PerformanceProfile) *RemoteUserNatMultiClient {
		multiClient := &RemoteUserNatMultiClient{
			generator: &testingEmptyMultiClientGenerator{},
		}
		multiClient.config.Store(&multiClientConfig{
			performanceProfile: performanceProfile,
		})
		return multiClient
	}

	// nil profile: auto
	multiClient := newMultiClient(nil)
	AssertEqual(t, []WindowType{WindowTypeQuality, WindowTypeSpeed}, multiClient.selectWindowTypes(webPacket, ""))
	AssertEqual(t, []WindowType{WindowTypeSpeed, WindowTypeQuality}, multiClient.selectWindowTypes(otherPacket, ""))

	// an auto profile selects the same windows as nil; the orthogonal
	// settings do not fix a window
	multiClient = newMultiClient(&PerformanceProfile{
		WindowType:  WindowTypeAuto,
		AllowDirect: true,
	})
	AssertEqual(t, []WindowType{WindowTypeQuality, WindowTypeSpeed}, multiClient.selectWindowTypes(webPacket, ""))
	AssertEqual(t, []WindowType{WindowTypeSpeed, WindowTypeQuality}, multiClient.selectWindowTypes(otherPacket, ""))

	// a fixed profile selects only its window type
	multiClient = newMultiClient(&PerformanceProfile{
		WindowType: WindowTypeSpeed,
		WindowSize: DefaultWindowSizeSettings(),
	})
	AssertEqual(t, []WindowType{WindowTypeSpeed}, multiClient.selectWindowTypes(webPacket, ""))
	AssertEqual(t, []WindowType{WindowTypeSpeed}, multiClient.selectWindowTypes(otherPacket, ""))
}

// TestMultiClientAutoWindowSizeIgnored verifies the profile window size only
// applies when the profile fixes a window: `FixedWindowSize=1` collapses
// affinity to a single path under a fixed profile, and is ignored under auto.
func TestMultiClientAutoWindowSizeIgnored(t *testing.T) {
	ipPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	}
	singleIpWindowSize := DefaultWindowSizeSettings()
	singleIpWindowSize.FixedWindowSize = 1

	// fixed profile with FixedWindowSize=1: single version-only affinity path
	multiClient := &RemoteUserNatMultiClient{}
	multiClient.config.Store(&multiClientConfig{
		performanceProfile: &PerformanceProfile{
			WindowType: WindowTypeQuality,
			WindowSize: singleIpWindowSize,
		},
	})
	paths := multiClient.affinityIpPathsWithLock(ipPath)
	AssertEqual(t, 1, len(paths))
	AssertEqual(t, &IpPath{Version: 4}, paths[0])

	// the same window size under auto is ignored: affinity cycles per
	// destination as if no profile were set
	multiClient = &RemoteUserNatMultiClient{}
	multiClient.config.Store(&multiClientConfig{
		performanceProfile: &PerformanceProfile{
			WindowType: WindowTypeAuto,
			WindowSize: singleIpWindowSize,
		},
	})
	paths = multiClient.affinityIpPathsWithLock(ipPath)
	AssertEqual(t, 1, len(paths))
	AssertEqual(t, &IpPath{
		Version:         4,
		DestinationIp:   ipPath.DestinationIp,
		DestinationPort: 443,
	}, paths[0])
}

// TestMultiClientNetworkAllowDirectAuto verifies the same-network force keeps
// an auto profile auto: it forces `AllowDirect` on without fixing a window,
// on both the constructor default path and the set path.
func TestMultiClientNetworkAllowDirectAuto(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultMultiClientSettings()
	settings.DefaultPerformanceProfile = &PerformanceProfile{
		WindowType: WindowTypeAuto,
	}
	multiClient := NewRemoteUserNatMultiClient(
		ctx,
		&testingEmptyMultiClientGenerator{},
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		},
		protocol.ProvideMode_Network,
		settings,
	)
	defer multiClient.Close()

	pp := multiClient.config.Load().performanceProfile
	AssertEqual(t, true, pp != nil)
	AssertEqual(t, true, pp.AllowDirect)
	AssertEqual(t, WindowTypeAuto, pp.WindowType)
	func() {
		qualityWindow := multiClient.windows[WindowTypeQuality]
		qualityWindow.stateLock.Lock()
		defer qualityWindow.stateLock.Unlock()

		pp = qualityWindow.performanceProfile
		AssertEqual(t, true, pp != nil)
		AssertEqual(t, true, pp.AllowDirect)
		AssertEqual(t, WindowTypeAuto, pp.WindowType)
	}()
	_, _, ok := pp.FixedWindow()
	AssertEqual(t, false, ok)

	multiClient.SetPerformanceProfile(&PerformanceProfile{
		WindowType: WindowTypeAuto,
	})
	pp = multiClient.config.Load().performanceProfile
	AssertEqual(t, true, pp != nil)
	AssertEqual(t, true, pp.AllowDirect)
	AssertEqual(t, WindowTypeAuto, pp.WindowType)
}

// TestMultiClientNetworkAllowDirectNilProfileStaysAuto verifies that forcing
// AllowDirect on with no profile set at all (DefaultPerformanceProfile nil)
// fabricates an auto profile, not a fixed WindowTypeQuality one — forcing
// direct mode must not pin the window as a side effect.
func TestMultiClientNetworkAllowDirectNilProfileStaysAuto(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultMultiClientSettings()
	multiClient := NewRemoteUserNatMultiClient(
		ctx,
		&testingEmptyMultiClientGenerator{},
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		},
		protocol.ProvideMode_Network,
		settings,
	)
	defer multiClient.Close()

	pp := multiClient.config.Load().performanceProfile
	AssertEqual(t, true, pp != nil)
	AssertEqual(t, true, pp.AllowDirect)
	AssertEqual(t, WindowTypeAuto, pp.WindowType)
	func() {
		qualityWindow := multiClient.windows[WindowTypeQuality]
		qualityWindow.stateLock.Lock()
		defer qualityWindow.stateLock.Unlock()

		pp = qualityWindow.performanceProfile
		AssertEqual(t, true, pp != nil)
		AssertEqual(t, true, pp.AllowDirect)
		AssertEqual(t, WindowTypeAuto, pp.WindowType)
	}()
	_, _, ok := pp.FixedWindow()
	AssertEqual(t, false, ok)
}

package connect

import (
	"testing"
	"time"
)

// the overrides must actually change behavior, or the developer menu's A/B is
// meaningless
func TestReliabilitySettingsOverrideTakesEffect(t *testing.T) {
	mc := &RemoteUserNatMultiClient{
		settings: &MultiClientSettings{
			UdpTeardownSignal:      true,
			SequenceIdleTimeout:    120 * time.Second,
			TcpSequenceIdleTimeout: 600 * time.Second,
		},
	}

	// constructed state
	_, ok := mc.teardownSourcePacket(udpTestPath(4), 0)
	AssertEqual(t, ok, true)
	AssertEqual(t, mc.sequenceIdleTimeout(idleTestPath(IpProtocolTcp)), 600*time.Second)

	// override turns the udp signal off and collapses the tcp idle bound
	mc.SetReliabilitySettings(&ReliabilitySettings{
		UdpTeardownSignal:   false,
		SequenceIdleTimeout: 30 * time.Second,
	})
	_, ok = mc.teardownSourcePacket(udpTestPath(4), 0)
	AssertEqual(t, ok, false)
	AssertEqual(t, mc.sequenceIdleTimeout(idleTestPath(IpProtocolTcp)), 30*time.Second)

	// clearing restores what the client was constructed with, so a menu can
	// always get back to the shipped behavior
	mc.SetReliabilitySettings(nil)
	_, ok = mc.teardownSourcePacket(udpTestPath(4), 0)
	AssertEqual(t, ok, true)
	AssertEqual(t, mc.sequenceIdleTimeout(idleTestPath(IpProtocolTcp)), 600*time.Second)
}

// reporting the live state back to the menu
func TestReliabilitySettingsReadback(t *testing.T) {
	mc := &RemoteUserNatMultiClient{
		settings: &MultiClientSettings{ClusterAffinityFallback: true, TcpCollapseMaxHold: time.Second},
	}

	AssertEqual(t, mc.ReliabilitySettings().ClusterAffinityFallback, true)
	AssertEqual(t, mc.ReliabilitySettings().TcpCollapseMaxHold, time.Second)

	mc.SetReliabilitySettings(&ReliabilitySettings{ClusterAffinityFallback: false})
	AssertEqual(t, mc.ReliabilitySettings().ClusterAffinityFallback, false)
	AssertEqual(t, mc.ReliabilitySettings().TcpCollapseMaxHold, time.Duration(0))
}

// a bare client must not panic -- the same invariant the cluster fallback and
// the idle timeout had to learn
func TestReliabilitySettingsBareClient(t *testing.T) {
	mc := &RemoteUserNatMultiClient{}

	reliabilitySettings := mc.ReliabilitySettings()
	AssertEqual(t, reliabilitySettings.UdpTeardownSignal, false)
	AssertEqual(t, reliabilitySettings.TcpCollapseMaxHold, time.Duration(0))
}

// nil settings yields every reliability behavior off, i.e. the state before any
// of this work
func TestReliabilitySettingsFromNil(t *testing.T) {
	reliabilitySettings := ReliabilitySettingsFrom(nil)
	AssertEqual(t, reliabilitySettings.UdpTeardownSignal, false)
	AssertEqual(t, reliabilitySettings.ClusterAffinityFallback, false)
	AssertEqual(t, reliabilitySettings.ServerNameAffinityBridge, false)
	AssertEqual(t, reliabilitySettings.TcpCollapseMaxHold, time.Duration(0))
}

// the shipped defaults must survive the round trip through the override type,
// or clearing an override would silently ship different behavior
func TestReliabilitySettingsFromDefaults(t *testing.T) {
	settings := DefaultMultiClientSettings()
	reliabilitySettings := ReliabilitySettingsFrom(settings)

	AssertEqual(t, reliabilitySettings.UdpTeardownSignal, settings.UdpTeardownSignal)
	AssertEqual(t, reliabilitySettings.TcpCollapseMaxHold, settings.TcpCollapseMaxHold)
	AssertEqual(t, reliabilitySettings.ClusterAffinityFallback, settings.ClusterAffinityFallback)
	AssertEqual(t, reliabilitySettings.ServerNameAffinityBridge, settings.ServerNameAffinityBridge)
	AssertEqual(t, reliabilitySettings.SequenceIdleTimeout, settings.SequenceIdleTimeout)
	AssertEqual(t, reliabilitySettings.TcpSequenceIdleTimeout, settings.TcpSequenceIdleTimeout)
	AssertEqual(t, reliabilitySettings.UplinkStalenessGate, settings.UplinkStalenessGate)
	AssertEqual(t, reliabilitySettings.SoftVerdictDemote, settings.SoftVerdictDemote)
	AssertEqual(t, reliabilitySettings.RemovalBudgetCount, settings.RemovalBudgetCount)
	AssertEqual(t, reliabilitySettings.RemovalBudgetWindow, settings.RemovalBudgetWindow)
	AssertEqual(t, reliabilitySettings.ProviderProbe, settings.ProviderProbe)
	AssertEqual(t, reliabilitySettings.ProbeTimeout, settings.ProbeTimeout)
}

// a stalled exit reports the packet as sent but never acknowledges it, and
// must not error -- an error would reset the flow immediately, which is the
// opposite of the state being reproduced
// It must also account for the packet as outstanding. Detection treats a
// client with nothing in flight as idle rather than broken, so a stalled exit
// that skips the accounting is invisible to sendStalled -- which is what
// happened on device, where a stall went unnoticed for 34s while every flow on
// that exit was dead.
func TestStalledChannelSwallowsWithoutError(t *testing.T) {
	settings := DefaultMultiClientSettings()
	// mirrors newMultiClientChannel: the stalled path now runs the real send
	// accounting rather than returning before it, so the channel needs the
	// state that accounting touches
	client := &multiClientChannel{
		settings:                  settings,
		packetStats:               &clientWindowStats{log: loggerOrDefault(settings.Log)},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
	}
	client.setStalled(true)

	success, err := client.SendDetailedWithAck(&parsedPacket{
		packet: make([]byte, 40),
		ipPath: udpTestPath(4),
	}, 0, true)

	AssertEqual(t, success, true)
	AssertEqual(t, err == nil, true)

	// the send is committed and will never be acknowledged, so the stall clock
	// is running and a timeout shorter than the elapsed time must trip. the
	// sleep is not incidental: the coarse clock on some platforms reports zero
	// elapsed for an immediate check, which would pass a broken implementation
	// as readily as a working one
	time.Sleep(5 * time.Millisecond)
	AssertEqual(t, client.sendStalled(1*time.Millisecond), true)
	// 0 still disables the check entirely
	AssertEqual(t, client.sendStalled(0), false)
}

// dropping or stalling an exit that is not in the window reports failure rather
// than silently doing nothing
func TestDropAndStallUnknownExit(t *testing.T) {
	mc := &RemoteUserNatMultiClient{windows: map[WindowType]*multiClientWindow{}}

	unknown := Id{}
	AssertEqual(t, mc.DropExit(unknown), false)
	AssertEqual(t, mc.StallExit(unknown, true), false)
}

// with no windows there are no exits, and the readout must not panic
func TestExitsEmpty(t *testing.T) {
	mc := &RemoteUserNatMultiClient{
		windows:       map[WindowType]*multiClientWindow{},
		clientUpdates: map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}
	AssertEqual(t, len(mc.Exits()), 0)
}

package connect

import "testing"

func newRtcTestDetector(settings *DmcaSecurityPolicySettings, webSettings *WebStandardSettings) *dmcaDetector {
	return newDmcaDetector(nil, settings, newWebStandardDetector(webSettings))
}

func TestDmcaStateMachineTurnChannelDataAllowed(t *testing.T) {
	settings := DefaultDmcaSecurityPolicySettings()
	detector := newRtcTestDetector(settings, DefaultWebStandardSettings())
	path := dmcaPath(IpProtocolUdp, 42000, 50000, false)

	// Deliberately exceed MaxInspectionPayload: frame-length validation must use
	// the complete datagram, while entropy work remains capped.
	frame := turnChannelData(0x4001, encryptedPayload(settings.MaxInspectionPayload+509), false)
	if verdict := detector.classify(path, frame); verdict != dmcaAllow {
		t.Fatalf("TURN ChannelData verdict = %d, want allow", verdict)
	}
	if verdict := detector.classify(path, encryptedPayload(512)); verdict != dmcaAllow {
		t.Fatalf("post-TURN encrypted packet verdict = %d, want terminal allow", verdict)
	}
}

func TestDmcaStateMachineTurnTcpAllowed(t *testing.T) {
	settings := DefaultDmcaSecurityPolicySettings()
	detector := newRtcTestDetector(settings, DefaultWebStandardSettings())
	syn := dmcaPath(IpProtocolTcp, 42001, 50000, true)
	if verdict := detector.classify(syn, nil); verdict != dmcaInspecting {
		t.Fatalf("TURN/TCP SYN verdict = %d, want inspecting", verdict)
	}
	data := dmcaPath(IpProtocolTcp, 42001, 50000, false)
	frame := turnChannelData(0x4001, encryptedPayload(509), true)
	if verdict := detector.classify(data, frame); verdict != dmcaAllow {
		t.Fatalf("TURN/TCP ChannelData verdict = %d, want allow", verdict)
	}
}

func TestDmcaStateMachineRtcpAllowed(t *testing.T) {
	detector := newRtcTestDetector(DefaultDmcaSecurityPolicySettings(), DefaultWebStandardSettings())
	path := dmcaPath(IpProtocolUdp, 42002, 50000, false)
	if verdict := detector.classify(path, rtcpPictureLossIndication(1, 2)); verdict != dmcaAllow {
		t.Fatalf("RTCP feedback verdict = %d, want allow", verdict)
	}
	if verdict := detector.classify(path, encryptedPayload(512)); verdict != dmcaAllow {
		t.Fatalf("post-RTCP encrypted packet verdict = %d, want terminal allow", verdict)
	}
}

func TestDmcaStateMachineRtpContinuityAllowed(t *testing.T) {
	settings := DefaultDmcaSecurityPolicySettings()
	detector := newRtcTestDetector(settings, DefaultWebStandardSettings())
	path := dmcaPath(IpProtocolUdp, 42003, 50000, false)
	first := rtpPacket(0x10203040, 1000, 48000, 111)
	second := rtpPacket(0x10203040, 1001, 48960, 111)
	if !payloadLooksEncrypted(first[:settings.MaxInspectionPayload], settings) {
		t.Fatal("RTP fixture must exercise the encrypted-payload heuristic")
	}
	if verdict := detector.classify(path, first); verdict != dmcaInspecting {
		t.Fatalf("first RTP packet verdict = %d, want inspecting", verdict)
	}
	if verdict := detector.classify(path, second); verdict != dmcaAllow {
		t.Fatalf("second coherent RTP packet verdict = %d, want allow", verdict)
	}
	if verdict := detector.classify(path, encryptedPayload(512)); verdict != dmcaAllow {
		t.Fatalf("post-RTP encrypted packet verdict = %d, want terminal allow", verdict)
	}
}

func TestDmcaStateMachineRtpSerialArithmetic(t *testing.T) {
	tests := []struct {
		name  string
		first []byte
		mid   []byte
		last  []byte
	}{
		{
			name:  "sequence and timestamp wrap",
			first: rtpPacket(7, 65535, 0xfffffff0, 111),
			last:  rtpPacket(7, 0, 0x00000020, 111),
		},
		{
			name:  "out of order packet ignored",
			first: rtpPacket(8, 100, 10000, 111),
			mid:   rtpPacket(8, 99, 9040, 111),
			last:  rtpPacket(8, 101, 10960, 111),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detector := newRtcTestDetector(DefaultDmcaSecurityPolicySettings(), DefaultWebStandardSettings())
			path := dmcaPath(IpProtocolUdp, 42004, 50000, false)
			if verdict := detector.classify(path, tt.first); verdict != dmcaInspecting {
				t.Fatalf("first verdict = %d, want inspecting", verdict)
			}
			if tt.mid != nil {
				if verdict := detector.classify(path, tt.mid); verdict != dmcaInspecting {
					t.Fatalf("middle verdict = %d, want inspecting", verdict)
				}
			}
			if verdict := detector.classify(path, tt.last); verdict != dmcaAllow {
				t.Fatalf("coherent final verdict = %d, want allow", verdict)
			}
		})
	}
}

func TestDmcaStateMachineInterleavedRtpSourcesAllowed(t *testing.T) {
	detector := newRtcTestDetector(DefaultDmcaSecurityPolicySettings(), DefaultWebStandardSettings())
	path := dmcaPath(IpProtocolUdp, 42005, 50000, false)
	packets := [][]byte{
		rtpPacket(0x11111111, 10, 1000, 111),
		rtpPacket(0x22222222, 20, 2000, 96),
		rtpPacket(0x11111111, 11, 1960, 111),
	}
	for i, packet := range packets {
		verdict := detector.classify(path, packet)
		if i < len(packets)-1 && verdict != dmcaInspecting {
			t.Fatalf("interleaved packet %d verdict = %d, want inspecting", i, verdict)
		}
		if i == len(packets)-1 && verdict != dmcaAllow {
			t.Fatalf("validated interleaved source verdict = %d, want allow", verdict)
		}
	}
}

func TestDmcaStateMachineRtpNearMissesDrop(t *testing.T) {
	tests := []struct {
		name   string
		first  []byte
		second []byte
	}{
		{
			name:   "duplicate sequence",
			first:  rtpPacket(1, 100, 1000, 111),
			second: rtpPacket(1, 100, 1000, 111),
		},
		{
			name:   "different ssrc",
			first:  rtpPacket(1, 100, 1000, 111),
			second: rtpPacket(2, 101, 1960, 111),
		},
		{
			name:   "different payload type",
			first:  rtpPacket(1, 100, 1000, 111),
			second: rtpPacket(1, 101, 1960, 96),
		},
		{
			name:   "implausible sequence jump",
			first:  rtpPacket(1, 100, 1000, 111),
			second: rtpPacket(1, 1000, 1960, 111),
		},
		{
			name:   "backward timestamp",
			first:  rtpPacket(1, 100, 2000, 111),
			second: rtpPacket(1, 101, 1000, 111),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detector := newRtcTestDetector(DefaultDmcaSecurityPolicySettings(), DefaultWebStandardSettings())
			path := dmcaPath(IpProtocolUdp, 42006, 50000, false)
			if verdict := detector.classify(path, tt.first); verdict != dmcaInspecting {
				t.Fatalf("first near-miss verdict = %d, want inspecting", verdict)
			}
			if verdict := detector.classify(path, tt.second); verdict != dmcaInspecting {
				t.Fatalf("second near-miss verdict = %d, want inspecting", verdict)
			}
			if verdict := detector.classify(path, encryptedPayload(512)); verdict != dmcaDropEncrypted {
				t.Fatalf("unvalidated RTP-like flow verdict = %d, want encrypted drop", verdict)
			}
		})
	}
}

func TestDmcaStateMachineRtpToggleAndTransport(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		web := DefaultWebStandardSettings()
		web.Rtp = false
		detector := newRtcTestDetector(DefaultDmcaSecurityPolicySettings(), web)
		path := dmcaPath(IpProtocolUdp, 42007, 50000, false)
		if verdict := detector.classify(path, rtpPacket(1, 1, 1000, 111)); verdict != dmcaInspecting {
			t.Fatalf("first disabled-RTP verdict = %d, want inspecting", verdict)
		}
		if verdict := detector.classify(path, rtpPacket(1, 2, 1960, 111)); verdict != dmcaInspecting {
			t.Fatalf("second disabled-RTP verdict = %d, want inspecting", verdict)
		}
		if verdict := detector.classify(path, encryptedPayload(512)); verdict != dmcaDropEncrypted {
			t.Fatalf("disabled-RTP flow verdict = %d, want encrypted drop", verdict)
		}
	})

	t.Run("tcp", func(t *testing.T) {
		detector := newRtcTestDetector(DefaultDmcaSecurityPolicySettings(), DefaultWebStandardSettings())
		syn := dmcaPath(IpProtocolTcp, 42008, 50000, true)
		if verdict := detector.classify(syn, nil); verdict != dmcaInspecting {
			t.Fatalf("SYN verdict = %d, want inspecting", verdict)
		}
		path := dmcaPath(IpProtocolTcp, 42008, 50000, false)
		packets := [][]byte{
			rtpPacket(1, 1, 1000, 111),
			rtpPacket(1, 2, 1960, 111),
			encryptedPayload(512),
		}
		var verdict dmcaVerdict
		for _, packet := range packets {
			verdict = detector.classify(path, packet)
		}
		if verdict != dmcaDropEncrypted {
			t.Fatalf("RTP-shaped TCP flow verdict = %d, want encrypted drop", verdict)
		}
	})
}

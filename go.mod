module github.com/urnetwork/connect

go 1.26.3

require (
	github.com/docopt/docopt-go v0.0.0-20180111231733-ee0de3bc6815
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/gopacket/gopacket v1.7.1
	github.com/gorilla/websocket v1.5.3
	github.com/pion/datachannel v1.6.2
	github.com/pion/ice/v4 v4.4.1
	github.com/pion/logging v0.2.4
	github.com/pion/rtp v1.10.5
	github.com/pion/transport/v4 v4.1.0
	github.com/pion/webrtc/v4 v4.2.18
	github.com/quic-go/quic-go v0.61.0
	github.com/rivo/uniseg v0.4.7
	github.com/urnetwork/glog v0.0.0
	github.com/wlynxg/anet v0.0.5
	golang.org/x/crypto v0.54.0
	golang.org/x/net v0.57.0
	golang.org/x/sys v0.47.0
	golang.org/x/text v0.40.0
	google.golang.org/protobuf v1.36.11
	gvisor.dev/gvisor v0.0.0-20260805230438-8eba670122c5
	src.agwa.name/tlshacks v0.0.4
)

require (
	github.com/google/btree v1.1.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/pion/dtls/v3 v3.1.5 // indirect
	github.com/pion/interceptor v0.1.47 // indirect
	github.com/pion/mdns/v2 v2.1.0 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/rtcp v1.2.17 // indirect
	github.com/pion/sctp v1.11.1 // indirect
	github.com/pion/sdp/v3 v3.0.19 // indirect
	github.com/pion/srtp/v3 v3.0.13 // indirect
	github.com/pion/stun/v3 v3.1.6 // indirect
	github.com/pion/turn/v5 v5.0.12 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	golang.org/x/exp v0.0.0-20260727155853-b88d891fe743 // indirect
	golang.org/x/time v0.15.0 // indirect
)

retract [v0.0.1, v1.0.0]

retract v0.2.0 // retract self

replace github.com/urnetwork/glog => ../glog

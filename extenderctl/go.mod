module bringyour.com/extenderctl

go 1.22.0

replace bringyour.com/connect v0.0.0 => ../connect

replace bringyour.com/protocol v0.0.0 => ../protocol/build/bringyour.com/protocol

require (
	bringyour.com/connect v0.0.0
	bringyour.com/protocol v0.0.0
	github.com/docopt/docopt-go v0.0.0-20180111231733-ee0de3bc6815
	github.com/golang-jwt/jwt/v5 v5.2.0
)

require (
	github.com/golang/glog v1.2.1 // indirect
	github.com/google/gopacket v1.1.19 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/oklog/ulid/v2 v2.1.0 // indirect
	golang.org/x/crypto v0.0.0-20220518034528-6f7dac969898 // indirect
	golang.org/x/exp v0.0.0-20230713183714-613f0c0eb8a1 // indirect
	golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2 // indirect
	golang.org/x/text v0.3.6 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	src.agwa.name/tlshacks v0.0.0-20231008131857-90d701ba3225 // indirect
)

module bringyour.com/connect

go 1.22.0

replace bringyour.com/wireguard v0.0.0 => ../../userspace-wireguard

require (
	bringyour.com/protocol v0.0.0
	bringyour.com/wireguard v0.0.0
	github.com/go-playground/assert/v2 v2.2.0
	github.com/golang-jwt/jwt/v5 v5.2.0
	github.com/golang/glog v1.2.1
	github.com/google/gopacket v1.1.19
	github.com/gorilla/websocket v1.5.0
	github.com/oklog/ulid/v2 v2.1.0
	golang.org/x/exp v0.0.0-20240506185415-9bf2ced13842
	golang.zx2c4.com/wireguard/wgctrl v0.0.0-20230429144221-925a1e7659e6
	google.golang.org/protobuf v1.34.2
)

require (
	golang.org/x/crypto v0.23.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
)

replace bringyour.com/protocol v0.0.0 => ../protocol/build/bringyour.com/protocol

module bringyour.com/connect

go 1.22.0

require (
	bringyour.com/protocol v0.0.0
	github.com/go-playground/assert/v2 v2.2.0
	github.com/golang-jwt/jwt/v5 v5.2.0
	github.com/google/gopacket v1.1.19
	github.com/gorilla/websocket v1.5.0
	github.com/oklog/ulid/v2 v2.1.0
	golang.org/x/exp v0.0.0-20230713183714-613f0c0eb8a1
	google.golang.org/protobuf v1.33.0
)

require github.com/golang/glog v1.2.1 // indirect

replace bringyour.com/protocol v0.0.0 => ../protocol/build/bringyour.com/protocol

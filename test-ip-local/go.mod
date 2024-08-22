module bringyour.com/connect/test-ip-local

go 1.22.0

replace bringyour.com/connect v0.0.0 => ../connect

replace github.com/bringyour/connect/protocol v0.0.0 => ../protocol/build/github.com/bringyour/connect/protocol

require (
	bringyour.com/connect v0.0.0 // indirect
	github.com/bringyour/connect/protocol v0.0.0 // indirect
	github.com/google/gopacket v1.1.19 // indirect
	github.com/oklog/ulid/v2 v2.1.0 // indirect
	golang.org/x/exp v0.0.0-20230713183714-613f0c0eb8a1 // indirect
	golang.org/x/sys v0.12.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

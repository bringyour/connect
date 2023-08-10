module bringyour.com/connect

go 1.21

require bringyour.com/protocol v0.0.0

require (
	github.com/google/gopacket v1.1.19 // indirect
	github.com/oklog/ulid/v2 v2.1.0 // indirect
	golang.org/x/exp v0.0.0-20230713183714-613f0c0eb8a1 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

replace bringyour.com/protocol v0.0.0 => ../protocol/build/bringyour.com/protocol

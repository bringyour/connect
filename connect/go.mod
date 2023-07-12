module bringyour.com/connect

go 1.20

require bringyour.com/protocol v0.0.0

require (
	github.com/oklog/ulid/v2 v2.1.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

replace bringyour.com/protocol v0.0.0 => ../protocol/build/bringyour.com/protocol

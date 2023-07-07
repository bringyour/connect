module bringyour.com/connect

go 1.20

require bringyour.com/protocol v0.0.0

require google.golang.org/protobuf v1.31.0 // indirect

replace bringyour.com/protocol v0.0.0 => ../protocol/build/bringyour.com/protocol

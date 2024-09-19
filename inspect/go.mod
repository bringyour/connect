module bringyour.com/inspect

go 1.22.5

replace bringyour.com/protocol v0.0.0 => ../protocol/build/bringyour.com/protocol

require (
	bringyour.com/protocol v0.0.0
	github.com/google/gopacket v1.1.19
	google.golang.org/protobuf v1.34.2
)

require (
	github.com/oklog/ulid/v2 v2.1.0
	golang.org/x/sys v0.0.0-20190412213103-97732733099d // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
)

module bringyour.com/inspect

go 1.22.5

replace bringyour.com/protocol v0.0.0 => ../protocol/build/bringyour.com/protocol

require (
	bringyour.com/protocol v0.0.0
	github.com/google/gopacket v1.1.19
	google.golang.org/protobuf v1.34.2
)

require (
	github.com/MaxHalford/eaopt v0.4.2
	github.com/humilityai/hdbscan v0.0.0-20200803015015-25a3a222c745
	golang.org/x/crypto v0.27.0
	gonum.org/v1/gonum v0.15.1
	src.agwa.name/tlshacks v0.0.0-20231008131857-90d701ba3225
)

require (
	golang.org/x/exp v0.0.0-20241009180824-f66d83c29e7c // indirect
	golang.org/x/sync v0.8.0 // indirect
)

require (
	github.com/oklog/ulid/v2 v2.1.0
	golang.org/x/sys v0.25.0 // indirect
)

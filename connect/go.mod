module bringyour.com/connect

go 1.22.0

require (
	bringyour.com/protocol v0.0.0
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/go-playground/assert/v2 v2.2.0
	github.com/golang-jwt/jwt/v5 v5.2.0
	github.com/google/gopacket v1.1.19
	github.com/gorilla/websocket v1.5.0
	github.com/jellydator/ttlcache/v3 v3.3.0
	github.com/oklog/ulid/v2 v2.1.0
	github.com/quic-go/quic-go v0.46.0
	github.com/stretchr/testify v1.9.0
	golang.org/x/crypto v0.23.0
	golang.org/x/exp v0.0.0-20240506185415-9bf2ced13842
	golang.org/x/net v0.25.0
	golang.org/x/sync v0.8.0
	google.golang.org/protobuf v1.33.0
	gvisor.dev/gvisor v0.0.0-20240912184249-d4480ca03532
	src.agwa.name/tlshacks v0.0.0-20231008131857-90d701ba3225
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/pprof v0.0.0-20210407192527-94a9f03dee38 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/onsi/ginkgo/v2 v2.9.5 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/mock v0.4.0 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.21.0 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/golang/glog v1.2.1
	golang.org/x/text v0.15.0 // indirect
)

replace bringyour.com/protocol v0.0.0 => ../protocol/build/bringyour.com/protocol

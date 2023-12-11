
all: clean build

clean:
	rm -rf build

build:
	mkdir build
	protoc -I=. --go_out=build *.proto
	cd build/bringyour.com/protocol; go mod init bringyour.com/protocol
	cd build/bringyour.com/protocol; go mod tidy

install_protoc:
	# install protoc from https://github.com/protocolbuffers/protobuf/releases
	# xattr -d com.apple.quarantine `which protoc`
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

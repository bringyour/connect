

default: init build

build: 
	protoc -I=. --go_out=. --go_opt=paths=source_relative *.proto

init:
	go install google.golang.org/protobuf/cmd/protoc-gen-go	

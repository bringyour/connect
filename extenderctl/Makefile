
all: clean build

clean:
	rm -rf build

build:
	env GOOS=linux GOARCH=arm64 go build -o build/linux/arm64/
	env GOOS=linux GOARCH=amd64 go build -o build/linux/amd64/
	env GOOS=darwin GOARCH=arm64 go build -o build/darwin/arm64/
	env GOOS=darwin GOARCH=amd64 go build -o build/darwin/amd64/


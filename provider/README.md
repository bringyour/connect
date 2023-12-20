# BringYour Provider Binary

This package implements a provider binary.

```
Usage:
    provider provide [--port=<port>] --user_auth=<user_auth> [--password=<password>]
        [--api_url=<api_url>]
        [--connect_url=<connect_url>]
```

It is set up to be build with `warpctl build` and push to the community build.

## Build and Run Locally

To build and run locally:

```
go build
./provider
```

## Community Build

BringYour hosts a build on DockerHub ([bringyour/community-provider](https://hub.docker.com/repository/docker/bringyour/community-provider/general)). You can set up Warp to follow this image so that when an image is pushed to block `g4`, your hosts will update to the new image and run the new code. Using the community build with Warp is completely optional, but it can be an easy way to keep your provider up to date with the latest. BringYour tests the builds on blocks `beta`, `g1`, `g2`, and `g3`. Of course you can follow those blocks also if you want to run more cutting edge code.

See [community-provider-warp-example/anyhost](community-provider-warp-example/anyhost) for example Warp systemd units. You will need to build and install `warpctl` on the target host.


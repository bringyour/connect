FROM ubuntu:22.04
# https://www.docker.com/blog/faster-multi-platform-builds-dockerfile-cross-compilation-guide/

ARG TARGETPLATFORM
ARG warp_env
ENV WARP_ENV=$warp_env

# see https://docs.docker.com/develop/develop-images/dockerfile_best-practices/
RUN apt-get update && apt-get install -y \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN update-ca-certificates

COPY build/$TARGETPLATFORM/provider /usr/local/sbin/bringyour-provider

EXPOSE 80
STOPSIGNAL SIGTERM
# additional arguments can be appended to the end
ENTRYPOINT ["/usr/local/sbin/bringyour-provider", "provide", "-p", "80"]

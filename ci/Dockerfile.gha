FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG GOLANG_VERSION=1.13.3
ARG TARGETARCH
ARG GO_SHA256_AMD64=0804bf02020dceaa8a7d7275ee79f7a142f1996bfd0c39216ccb405f93f994c0
ARG GO_SHA256_ARM64=9fa65ae42665baff53802091b49b83af6f2e397986b6cbea2ae30e2c7ee0f2f2

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        git \
        protobuf-compiler \
        libprotobuf-dev \
        libgrpc-dev \
        libgrpc++-dev \
        libc-ares-dev \
        protobuf-compiler-grpc \
        libpoco-dev \
        wget \
    && rm -rf /var/lib/apt/lists/*

RUN case "${TARGETARCH}" in \
      amd64) GO_ARCH="${TARGETARCH}"; GO_SHA256="${GO_SHA256_AMD64}" ;; \
      arm64) GO_ARCH="${TARGETARCH}"; GO_SHA256="${GO_SHA256_ARM64}" ;; \
      "") GO_ARCH="amd64"; GO_SHA256="${GO_SHA256_AMD64}" ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && wget -q -O /tmp/go.tgz "https://dl.google.com/go/go${GOLANG_VERSION}.linux-${GO_ARCH}.tar.gz" \
    && echo "${GO_SHA256}  /tmp/go.tgz" | sha256sum -c - \
    && tar -C /usr/local -xzf /tmp/go.tgz \
    && rm -f /tmp/go.tgz

ENV GOPATH=/go
ENV PATH=/usr/local/go/bin:${GOPATH}/bin:${PATH}
ENV HOME=/home/ciuser

RUN groupadd --system ciuser \
    && useradd --system --create-home --gid ciuser ciuser \
    && mkdir -p /go /mock-tikv \
    && chown -R ciuser:ciuser /go /mock-tikv

USER ciuser

RUN git clone --depth 1 https://github.com/tikv/mock-tikv.git /mock-tikv \
    && cd /mock-tikv \
    && git fetch --depth 1 origin 60d5921028afd72e1aeba880b9052c40e932eef3 \
    && git checkout 60d5921028afd72e1aeba880b9052c40e932eef3 \
    && make failpoint-enable \
    && make

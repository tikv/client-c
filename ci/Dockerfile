FROM ubuntu:21.04

RUN apt update -y \
 && apt install -y cmake build-essential \
        wget git \
        protobuf-compiler libprotobuf-dev libgrpc-dev libgrpc++-dev libc-ares-dev protobuf-compiler-grpc libpoco-dev

RUN rm -rf /var/lib/apt/lists/*

#back to root dir and download golang
RUN cd / 

ENV GOLANG_VERSION 1.13.3

RUN wget -O go.tgz "https://dl.google.com/go/go$GOLANG_VERSION.linux-amd64.tar.gz"; \
tar -C /usr/local -xzf go.tgz; \
rm go.tgz; \
export PATH="/usr/local/go/bin:$PATH"; \
go version

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN cd /

RUN git clone https://github.com/tikv/mock-tikv.git && cd mock-tikv && git checkout 388e21d3bab5d3c4d3ce26e643ab7453188b0288 && make failpoint-enable && make

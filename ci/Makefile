BUILDER_IMAGE := $(or $(BUILDER_IMAGE),hub.pingcap.net/pingcap/tikv-client-c-builder)

image:
	docker build -f Dockerfile -t $(BUILDER_IMAGE) .

build-test:
	docker run --rm -v $(realpath ..):/client-c $(BUILDER_IMAGE) /client-c/ci/build-test.sh
default: image build-test

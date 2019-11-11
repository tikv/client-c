default: image build-test

BUILDER_IMAGE := $(or $(BUILDER_IMAGE),hub.pingcap.net/tikv/client-c)

image:
	docker build -f Dockerfile -t $(BUILDER_IMAGE) .
build-test:
	docker run --rm -v $(realpath ..):/client-c $(BUILDER_IMAGE) /client-c/ci/build-test.sh

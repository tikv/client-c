default: image build-test

BUILDER_IMAGE := $(or $(BUILDER_IMAGE),hub.pingcap.net/tikv/client-c)

image:
	docker build -f Dockerfile -t $(BUILDER_IMAGE) .
build-test:
<<<<<<< HEAD
	docker run --rm -v $(realpath ..):/client-c $(BUILDER_IMAGE) /client-c/ci/build-test.sh
=======
	docker run --rm -v $(realpath ..):/client-c hub.pingcap.net/tikv/client-c /client-c/ci/build-test.sh
default: image build-test
>>>>>>> 1cf3487... add default target

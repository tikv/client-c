default: image build-test

image:
	docker build -f Dockerfile -t hub.pingcap.net/tikv/client-c .
build-test:
	docker run --rm -v $(realpath ..):/client-c hub.pingcap.net/tikv/client-c /client-c/ci/build-test.sh

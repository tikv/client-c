#!/bin/bash

set -xe


SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=$(cd $SCRIPTPATH/..; pwd -P)
NPROC=$(nproc || grep -c ^processor /proc/cpuinfo)

build_dir="$SRCPATH/build"
mkdir -p $build_dir && cd $build_dir
cmake "$SRCPATH" \
    -DENABLE_TESTS=on
make -j $NPROC

nohup /mock-tikv/bin/mock-tikv &
mock_kv_pid=$!

for i in {1..100}; do
    if (echo >/dev/tcp/127.0.0.1/2378) >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done

if ! (echo >/dev/tcp/127.0.0.1/2378) >/dev/null 2>&1; then
    echo "mock-tikv did not start listening on 127.0.0.1:2378"
    kill -9 $mock_kv_pid || true
    exit 1
fi

cd "$build_dir" && make test

kill -9 $mock_kv_pid


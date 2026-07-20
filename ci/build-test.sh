#!/bin/bash

set -xe


SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=$(cd $SCRIPTPATH/..; pwd -P)
NPROC=$(nproc || grep -c ^processor /proc/cpuinfo)

waitForMockTiKV() {
    local max_wait_seconds=30

    for ((i = 1; i <= max_wait_seconds; i++)); do
        if ! kill -0 "$mock_kv_pid" 2>/dev/null; then
            echo "mock-tikv exited before becoming ready"
            wait "$mock_kv_pid" || true
            return 1
        fi

        if (echo > /dev/tcp/127.0.0.1/2378) >/dev/null 2>&1; then
            echo "mock-tikv is ready"
            return 0
        fi

        sleep 1
    done

    echo "mock-tikv is not ready after ${max_wait_seconds}s"
    return 1
}

mock_kv_pid=""
cleanupMockTiKV() {
    if [ -n "$mock_kv_pid" ]; then
        kill -9 "$mock_kv_pid" 2>/dev/null || true
    fi
}
trap cleanupMockTiKV EXIT

build_dir="$SRCPATH/build"
mkdir -p $build_dir && cd $build_dir
cmake "$SRCPATH" \
    -DENABLE_TESTS=on
make -j $NPROC

nohup /mock-tikv/bin/mock-tikv &
mock_kv_pid=$!
waitForMockTiKV

cd "$build_dir" && make test

# Overview

TiKV Client C++ allow you to access [TiKV](https://github.com/tikv/tikv) from C and C++ applications.

The code is in an early alpha status. Currently, it is only used by TiFlash.

# License

[Apache-2.0 License](/LICENSE)

# Docs

The docs can be found [here](https://tikv.org/docs/dev/develop/clients/cpp/).

# Building

Install dependencies (adjust for your platform):
```
sudo dnf install cmake grpc-devel poco-devel abseil-cpp-devel gcc-c++
```

Update submodules
```
git submodule update --init --recursive
```

Build:
```
mkdir build
cd build
cmake ..
make
```

#!/bin/bash
clang-format -i ./include/pingcap/kv/*.h
clang-format -i ./include/pingcap/coprocessor/*.h
clang-format -i ./include/pingcap/kv/internal/*.h
clang-format -i ./include/pingcap/pd/*.h
clang-format -i ./include/pingcap/common/*.h
clang-format -i ./include/pingcap/*.h
clang-format -i ./src/kv/*.cc
clang-format -i ./src/coprocessor/*.cc
clang-format -i ./src/pd/*.cc
clang-format -i ./src/common/*.cc
clang-format -i ./src/test/*.cc
clang-format -i ./src/test/*.h

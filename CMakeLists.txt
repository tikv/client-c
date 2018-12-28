cmake_minimum_required(VERSION 2.8)
project(kvClient)

include (cmake/find_grpc.cmake)
include (cmake/find_poco.cmake)

if (NOT KVPROTO_FOUND)
    include (cmake/find_kvproto.cmake)
    add_subdirectory (third_party/kvproto/cpp)
endif (NOT KVPROTO_FOUND)

add_subdirectory (src)

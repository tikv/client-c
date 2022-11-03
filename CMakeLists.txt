cmake_minimum_required(VERSION 3.10)
project(kvClient)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wno-narrowing")
set (CMAKE_EXPORT_COMPILE_COMMANDS ON)
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")
enable_testing()

if (NOT Protobuf_INCLUDE_DIR)
    include (cmake/find_protobuf.cmake)
endif ()

if (NOT gRPC_FOUND)
    include (cmake/find_grpc.cmake)
endif ()

if (NOT Poco_Foundation_LIBRARY)
    include (cmake/find_poco.cmake)
endif()

if (NOT KVPROTO_FOUND)
    include (cmake/find_kvproto.cmake)
endif ()

if (NOT GTEST_FOUND AND ENABLE_TESTS)
    include (cmake/find_gtest.cmake)
endif()

add_subdirectory (third_party)
add_subdirectory (src)

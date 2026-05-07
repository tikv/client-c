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

set(TICI_PROTO_DIR "${kvClient_SOURCE_DIR}/proto")
set(TICI_PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/kvproto")
file(MAKE_DIRECTORY ${TICI_PROTO_OUTPUT_DIR})

set(TICI_PROTO_DEFS
    "${TICI_PROTO_DIR}/kvproto/schema.proto"
    "${TICI_PROTO_DIR}/kvproto/tici.proto"
)
set(TICI_PROTO_SRCS
    "${TICI_PROTO_OUTPUT_DIR}/schema.pb.cc"
    "${TICI_PROTO_OUTPUT_DIR}/tici.pb.cc"
    "${TICI_PROTO_OUTPUT_DIR}/tici.grpc.pb.cc"
)
set(TICI_PROTO_HDRS
    "${TICI_PROTO_OUTPUT_DIR}/schema.pb.h"
    "${TICI_PROTO_OUTPUT_DIR}/tici.pb.h"
    "${TICI_PROTO_OUTPUT_DIR}/tici.grpc.pb.h"
)

add_custom_command(
    OUTPUT ${TICI_PROTO_SRCS} ${TICI_PROTO_HDRS}
    WORKING_DIRECTORY ${TICI_PROTO_DIR}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR} -I=${TICI_PROTO_DIR} kvproto/schema.proto kvproto/tici.proto
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --grpc_out=${CMAKE_CURRENT_BINARY_DIR} -I=${TICI_PROTO_DIR} --plugin=protoc-gen-grpc=${gRPC_CPP_PLUGIN} kvproto/tici.proto
    DEPENDS ${TICI_PROTO_DEFS} ${Protobuf_PROTOC_EXECUTABLE} ${gRPC_CPP_PLUGIN}
    COMMENT "Generating TiCI C++ proto files"
    VERBATIM
)

add_library(tici_proto STATIC ${TICI_PROTO_SRCS} ${TICI_PROTO_HDRS})
target_include_directories(tici_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Protobuf_INCLUDE_DIR}
    ${gRPC_INCLUDE_DIRS}
)
target_link_libraries(tici_proto PUBLIC ${Protobuf_LIBRARY} ${gRPC_LIBRARIES} absl::synchronization)
set(TICI_PROTO_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}")

add_subdirectory (src)

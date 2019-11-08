add_compile_definitions(ENABLE_TESTS)

list (APPEND test_includes ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR})

list(APPEND test_libs
    kvproto
    kv_client
    ${Protobuf_LIBRARY}
    ${Poco_Net_LIBRARY}
    PocoJSON
    gRPC::grpc++_unsecure)

add_executable(kv_client_ut io_or_region_error_get_test.cc region_split_test.cc)
target_include_directories(kv_client_ut PUBLIC ${test_includes})
target_link_libraries(kv_client_ut ${test_libs} gtest_main)

include(CTest)
add_test(kv_client_test kv_client_ut)

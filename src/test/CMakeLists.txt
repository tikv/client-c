list (APPEND test_includes ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR} . )

list(APPEND test_libs
    kvproto
    kv_client
    ${Protobuf_LIBRARY}
    ${Poco_Net_LIBRARY}
    PocoJSON
    gtest_main
    fiu
    gRPC::grpc++_unsecure)

add_executable(kv_client_ut lock_resolve_test.cc io_or_region_error_get_test.cc region_split_test.cc)
target_include_directories(kv_client_ut PUBLIC ${test_includes})
target_link_libraries(kv_client_ut ${test_libs})

include(CTest)
add_test(kv_client_test kv_client_ut)

add_subdirectory(bank_test)
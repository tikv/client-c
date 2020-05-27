list (APPEND test_includes ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR} ${fiu_include_dirs})

list(APPEND test_libs
    kvproto
    kv_client
    ${Protobuf_LIBRARY}
    Poco::Net
    Poco::JSON
    gtest_main
    fiu
    gRPC::grpc++_unsecure)

add_executable(kv_client_ut io_or_region_error_get_test.cc region_split_test.cc lock_resolve_test.cc coprocessor_test.cc)
target_include_directories(kv_client_ut PUBLIC ${test_includes})
target_link_libraries(kv_client_ut ${test_libs} )

include(CTest)
add_test(kv_client_test kv_client_ut)

add_subdirectory(bank_test)
add_subdirectory(real_tikv_test)

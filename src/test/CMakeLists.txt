list (APPEND test_includes ${kvClient_INCLUDE_DIR} ${fiu_include_dirs})

list(APPEND test_libs
    kvproto
    kv_client
    ${Protobuf_LIBRARY}
    ${gRPC_LIBRARIES}
    ${Poco_Net_LIBRARY}
    ${Poco_JSON_LIBRARY}
    gtest_main
    fiu)

add_executable(kv_client_ut
    io_or_region_error_get_test.cc
    region_split_test.cc
    lock_resolve_test.cc
    coprocessor_test.cc
    batch_coprocessor_test.cc
    shard_cache_test.cc
)
target_include_directories(kv_client_ut PUBLIC ${test_includes})
target_link_libraries(kv_client_ut ${test_libs} )
target_compile_definitions(kv_client_ut PRIVATE PINGCAP_KV_CLIENT_TESTING=1)

include(CTest)
add_test(kv_client_test kv_client_ut)

add_subdirectory(bank_test)
add_subdirectory(real_tikv_test)

add_compile_definitions(ENABLE_TESTS)

list (APPEND test_includes ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR})

list(APPEND test_libs
    kvproto
    kv_client
    ${Protobuf_LIBRARY}
    ${Poco_Net_LIBRARY}
    PocoJSON
    gRPC::grpc++_unsecure)

add_executable(io_or_region_error_get_test io_or_region_error_get_test.cc)
target_include_directories(io_or_region_error_get_test PUBLIC ${test_includes})
target_link_libraries(io_or_region_error_get_test ${test_libs})

add_executable(region_split_get_test region_split_get_test.cc)
target_link_libraries(region_split_get_test ${test_libs})
target_include_directories(region_split_get_test PUBLIC ${test_includes})

add_executable(region_split_scan_test region_split_scan_test.cc)
target_link_libraries(region_split_scan_test ${test_libs})
target_include_directories(region_split_scan_test PUBLIC ${test_includes})

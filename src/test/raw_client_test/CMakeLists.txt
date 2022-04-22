add_executable(raw_client test_raw_client.cc)
target_include_directories(raw_client PUBLIC ${test_includes})
target_link_libraries(raw_client ${test_libs})

add_executable(stress stress.cc)
target_include_directories(stress PUBLIC ${test_includes})
target_link_libraries(stress ${test_libs})
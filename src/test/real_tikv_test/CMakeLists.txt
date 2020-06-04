add_executable(2pc_test 2pc_test.cc)
target_include_directories(2pc_test PUBLIC ${test_includes})
target_link_libraries(2pc_test ${test_libs})

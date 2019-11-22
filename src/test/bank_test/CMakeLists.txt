add_executable(bank_test_ut bank_test_ut.cc)
target_include_directories(bank_test_ut PUBLIC ${test_includes})
target_link_libraries(bank_test_ut ${test_libs})

add_test(bank_test bank_test_ut)

add_executable(bank_test_schrodinger bank_test_schrodinger.cc)
target_include_directories(bank_test_schrodinger PUBLIC ${test_includes})
target_link_libraries(bank_test_schrodinger ${test_libs})

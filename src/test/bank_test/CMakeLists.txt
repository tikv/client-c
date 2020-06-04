add_executable(bank_test_ut bank_test_ut.cc)
target_include_directories(bank_test_ut PUBLIC ${test_includes})
target_link_libraries(bank_test_ut ${test_libs})

add_test(bank_test bank_test_ut)

add_executable(bank_test_schrodinger bank_test_schrodinger.cc)
set(CMAKE_SHARED_LINKER_FLAGS "-static")
target_include_directories(bank_test_schrodinger PUBLIC ${test_includes})

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_libraries(bank_test_schrodinger -static-libgcc -static-libstdc++ ${test_libs} PocoUtil PocoXML PocoJSONd)
else()
    target_link_libraries(bank_test_schrodinger ${test_libs} Poco::Util Poco::XML Poco::JSON)
endif()


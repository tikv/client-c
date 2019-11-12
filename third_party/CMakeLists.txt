if (NOT KVPROTO_FOUND)
    add_subdirectory (kvproto/cpp)
endif()

if (USE_INTERNAL_GTEST_LIBRARY AND ENABLE_TESTS)
    add_subdirectory (googletest)
endif()

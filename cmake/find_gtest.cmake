
if (NOT EXISTS "${kvClient_SOURCE_DIR}/third_party/googletest/CMakeLists.txt")
    set(USE_INTERNAL_GTEST_LIBRARY 0)
    find_package(PkgConfig)
    pkg_search_module(GTEST REQUIRED gtest_main)
else ()
    set(BUILD_GMOCK 0)
    set(INSTALL_GTEST 0)
    set(USE_INTERNAL_GTEST_LIBRARY 1)
endif()

message(STATUS "Using Gtest: ${gtest_INCLUDE_DIRS} ${USE_INTERNAL_GTEST_LIBRARY}")

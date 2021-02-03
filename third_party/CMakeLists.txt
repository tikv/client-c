if (NOT KVPROTO_FOUND)
    add_subdirectory (kvproto/cpp)
endif()

if (USE_INTERNAL_GTEST_LIBRARY AND ENABLE_TESTS)
    set(GTEST_INSTALL OFF)
    add_subdirectory (googletest)
endif ()

if ( USE_INTERNAL_POCO_LIBRARY )
    set ( SAVED_ENABLE_TESTS )
    if ( ENABLE_TESTS )
        set ( ENABLE_TESTS OFF CACHE BOOL "" FORCE )
        set ( SAVED_ENABLE_TESTS 1 )
    endif ()

    add_subdirectory ( poco )

    if ( SAVED_ENABLE_TESTS )
        set ( ENABLE_TESTS ON CACHE BOOL "" FORCE )
        unset ( SAVED_ENABLE_TESTS )
    endif ()
endif ()

set(fiu_include_dirs libfiu/libfiu)
list(APPEND fiu_sources libfiu/libfiu/fiu.c)
list(APPEND fiu_sources libfiu/libfiu/fiu-rc.c)
list(APPEND fiu_sources libfiu/libfiu/backtrace.c)
list(APPEND fiu_sources libfiu/libfiu/wtable.c)
list(APPEND fiu_sources libfiu/libfiu/hash.c)
# We need to compile fiu manully because fiu does not provide their cmakefiles.
add_library(fiu ${fiu_sources})
target_include_directories(fiu PUBLIC ${fiu_include_dirs})
target_compile_options(fiu PUBLIC -fPIC -DDUMMY_BACKTRACE)

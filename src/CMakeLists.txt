set(kvClient_sources)

list(APPEND kvClient_sources kv/2pc.cc)
list(APPEND kvClient_sources kv/Backoff.cc)
list(APPEND kvClient_sources kv/Cluster.cc)
list(APPEND kvClient_sources kv/LockResolver.cc)
list(APPEND kvClient_sources kv/RegionCache.cc)
list(APPEND kvClient_sources kv/RegionClient.cc)
list(APPEND kvClient_sources kv/Rpc.cc)
list(APPEND kvClient_sources kv/Snapshot.cc)
list(APPEND kvClient_sources kv/Scanner.cc)
list(APPEND kvClient_sources pd/Client.cc)
list(APPEND kvClient_sources coprocessor/Client.cc)
list(APPEND kvClient_sources RedactHelpers.cc)

set(kvClient_INCLUDE_DIR ${kvClient_SOURCE_DIR}/include)

# Only enable fiu under test mode.
if (ENABLE_TESTS)
    add_definitions(-DFIU_ENABLE)
endif()

add_library(kv_client ${kvClient_sources})
target_include_directories(kv_client PUBLIC ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR} ${fiu_include_dirs})
target_link_libraries(kv_client kvproto ${Poco_Foundation_LIBRARY} fiu ${_GRPC_GRPCPP})

if (ENABLE_TESTS)
    add_subdirectory (test)
endif()

set(kvClient_sources kv/LockResolver.cc kv/Cluster.cc ../include/pingcap/kv/codec_pd_client.h)

list(APPEND kvClient_sources pd/Client.cc)
list(APPEND kvClient_sources kv/Region.cc)
list(APPEND kvClient_sources kv/RegionClient.cc)
list(APPEND kvClient_sources kv/Snapshot.cc)
list(APPEND kvClient_sources kv/Scanner.cc)
list(APPEND kvClient_sources kv/Backoff.cc)
list(APPEND kvClient_sources kv/Rpc.cc)
list(APPEND kvClient_sources kv/2pc.cc)

set(kvClient_INCLUDE_DIR ${kvClient_SOURCE_DIR}/include)

if (ENABLE_TESTS)
    add_definitions(-DFIU_ENABLE)
endif()

add_library(kv_client ${kvClient_sources})
target_include_directories(kv_client PUBLIC ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR} ${fiu_include_dirs})
target_link_libraries(kv_client kvproto ${Poco_Foundation_LIBRARY} fiu)

if (ENABLE_TESTS)
    add_subdirectory (test)
endif()

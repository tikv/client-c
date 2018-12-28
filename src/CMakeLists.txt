add_subdirectory (pd)

set(kvClient_sources)

list(APPEND kvClient_sources pd/Client.cc)
list(APPEND kvClient_sources common/Region.cc)
list(APPEND kvClient_sources common/RegionClient.cc)
list(APPEND kvClient_sources common/Backoff.cc)
list(APPEND kvClient_sources common/Rpc.cc)

add_library(kv_client ${kvClient_sources})
target_include_directories(kv_client PUBLIC ${KVPROTO_INCLUDE_DIR} ${kvClient_SOURCE_DIR}/include)
target_link_libraries(kv_client kvproto ${Poco_Foundation_LIBRARY})

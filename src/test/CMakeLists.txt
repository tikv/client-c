add_executable(pd_test pd_test.cc)

target_include_directories(pd_test PUBLIC ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR})

target_link_libraries(pd_test kvproto
                              kv_client
                              ${Protobuf_LIBRARY}
                              gRPC::grpc++_unsecure)

add_executable(get_store_id_wrong_test get_store_id_wrong_test.cc)
target_include_directories(get_store_id_wrong_test PUBLIC ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR})
target_link_libraries(get_store_id_wrong_test kvproto
                              kv_client
                              ${Protobuf_LIBRARY}
                              gRPC::grpc++_unsecure)

add_executable(get_store_id_wrong_test2 get_store_id_wrong_2_test.cc)
target_include_directories(get_store_id_wrong_test2 PUBLIC ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR})
target_link_libraries(get_store_id_wrong_test2 kvproto
                              kv_client
                              ${Protobuf_LIBRARY}
                              gRPC::grpc++_unsecure)

add_executable(get_region_id_wrong_test get_region_id_wrong_test.cc)
target_include_directories(get_region_id_wrong_test PUBLIC ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR})
target_link_libraries(get_region_id_wrong_test kvproto
                              kv_client
                              ${Protobuf_LIBRARY}
                              gRPC::grpc++_unsecure)

add_executable(server_is_busy_get_test server_is_busy_get_test.cc)
target_include_directories(server_is_busy_get_test PUBLIC ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR})
target_link_libraries(server_is_busy_get_test kvproto
                              kv_client
                              ${Protobuf_LIBRARY}
                              ${Poco_Net_LIBRARY}
                              ${Poco_Foundation_LIBRARY}
                              PocoJSON
                              gRPC::grpc++_unsecure)

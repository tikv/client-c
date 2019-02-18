add_executable(pd_test pd_test.cc)

target_include_directories(pd_test PUBLIC ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR})

target_link_libraries(pd_test kvproto
                              kv_client
                              ${Protobuf_LIBRARY}
                              gRPC::grpc++_unsecure)

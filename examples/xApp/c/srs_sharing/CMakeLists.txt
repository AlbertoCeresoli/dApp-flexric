# [srs-sharing] SRS sharing xApp. Its messages are protobuf (srs_sharing.proto, next to the E3 service models of
# the dApp SM), compiled here with protoc-c. Without protobuf-c the xApp is skipped.
find_program(PROTOC_C_EXECUTABLE protoc-c)
find_library(PROTOBUF_C_LIBRARY protobuf-c)

if(NOT PROTOC_C_EXECUTABLE OR NOT PROTOBUF_C_LIBRARY)
  message(STATUS "protobuf-c not found (apt install protobuf-c-compiler libprotobuf-c-dev): xapp_srs_sharing not built")
  return()
endif()

set(SRS_SHARING_PROTO_DIR ${CMAKE_SOURCE_DIR}/src/sm/dapp_sm/e3/service_models/srs_sharing_sm)
set(SRS_SHARING_GENERATED ${CMAKE_CURRENT_BINARY_DIR}/srs_sharing.pb-c.c ${CMAKE_CURRENT_BINARY_DIR}/srs_sharing.pb-c.h)
add_custom_command(
  OUTPUT ${SRS_SHARING_GENERATED}
  COMMAND ${PROTOC_C_EXECUTABLE} --proto_path=${SRS_SHARING_PROTO_DIR} --c_out=${CMAKE_CURRENT_BINARY_DIR}
          ${SRS_SHARING_PROTO_DIR}/srs_sharing.proto
  DEPENDS ${SRS_SHARING_PROTO_DIR}/srs_sharing.proto
  COMMENT "protoc-c srs_sharing.proto")

# The registry (UEs, timing advance ledger, updates) and the reorder buffer have no FlexRIC dependency, so they are
# tested on their own.
add_library(srs_sharing_registry STATIC srs_sharing_registry.c srs_sharing_reorder.c
            ${CMAKE_CURRENT_BINARY_DIR}/srs_sharing.pb-c.c)
target_include_directories(srs_sharing_registry PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(srs_sharing_registry PUBLIC ${PROTOBUF_C_LIBRARY})

add_executable(xapp_srs_sharing
  xapp_srs_sharing.c
  ../../../../src/util/alg_ds/alg/defer.c
)
target_link_libraries(xapp_srs_sharing PUBLIC
  e42_xapp
  srs_sharing_registry
  -pthread
  -lsctp
  -ldl
)

add_executable(test_srs_sharing_registry test_srs_sharing_registry.c)
target_link_libraries(test_srs_sharing_registry PRIVATE srs_sharing_registry)
# Built with NDEBUG in Release: keep the test's asserts.
target_compile_options(test_srs_sharing_registry PRIVATE -UNDEBUG)
add_test(NAME test_srs_sharing_registry COMMAND test_srs_sharing_registry)

add_executable(test_srs_sharing_reorder test_srs_sharing_reorder.c)
target_link_libraries(test_srs_sharing_reorder PRIVATE srs_sharing_registry)
target_compile_options(test_srs_sharing_reorder PRIVATE -UNDEBUG)
add_test(NAME test_srs_sharing_reorder COMMAND test_srs_sharing_reorder)

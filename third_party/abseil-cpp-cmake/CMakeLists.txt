set(ABSL_ROOT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../abseil-cpp")
set(ABSL_ROOT_DIR "${ABSL_ROOT_DIR}" PARENT_SCOPE)
if(NOT EXISTS "${ABSL_ROOT_DIR}/CMakeLists.txt")
  message(FATAL_ERROR " submodule third_party/abseil-cpp is missing. To fix try run: \n git submodule update --init --recursive")
endif()
set(BUILD_TESTING OFF)
set(ABSL_PROPAGATE_CXX_STD ON)

add_subdirectory("${ABSL_ROOT_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/../abseil-cpp")

add_library(abseil_swiss_tables INTERFACE)
set_target_properties(abseil_swiss_tables PROPERTIES INTERFACE_COMPILE_OPTIONS "-Wno-array-parameter")

target_link_libraries(abseil_swiss_tables INTERFACE
  absl::flat_hash_map
  absl::flat_hash_set
)

get_target_property(FLAT_HASH_MAP_INCLUDE_DIR absl::flat_hash_map INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories (abseil_swiss_tables SYSTEM BEFORE INTERFACE ${FLAT_HASH_MAP_INCLUDE_DIR})

get_target_property(FLAT_HASH_SET_INCLUDE_DIR absl::flat_hash_set INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories (abseil_swiss_tables SYSTEM BEFORE INTERFACE ${FLAT_HASH_SET_INCLUDE_DIR})

message(STATUS "Using absl: dir=${ABSL_ROOT_DIR}")

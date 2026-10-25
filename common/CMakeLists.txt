# fleet_common: small header-only helpers shared by the binaries (shutdown
# signal handling, timestamped log lines, the leader-redirect convention).
# INTERFACE because there is nothing to compile.
add_library(fleet_common INTERFACE)
target_include_directories(fleet_common INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

# fleet_io (m4b): checksummed frames, fsync rules and atomic file replace,
# shared by the master's write-ahead log (m4) and the Raft log (m4b). Pure
# POSIX, no gRPC or Protobuf.
add_library(fleet_io durable_io.cpp)
target_include_directories(fleet_io PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fleet_io PRIVATE fleet_flags)

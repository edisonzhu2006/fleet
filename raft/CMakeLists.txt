# fleet_raft (m4b): Raft consensus. RaftCore is the pure state machine
# (no threads, sockets or files; driven by tests/raft_sim_test.cpp's
# deterministic simulation and by the replica); RaftStorage makes its
# persistent state durable (frames and fsync from fleet_io); RaftTransport
# carries its messages over gRPC. Nothing here knows about fleet's tasks
# or nodes: a command is opaque bytes.
add_library(fleet_raft raft_core.cpp raft_storage.cpp raft_transport.cpp)
target_include_directories(fleet_raft PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fleet_raft PUBLIC fleet_proto fleet_io Threads::Threads PRIVATE fleet_flags)

# fleet_master_lib: MasterCore (logic, no threads, no network), its
# FailureDetector (m3), MasterState (durable state + apply, m4), Storage
# (write-ahead log + snapshot, m4), MasterServer (gRPC service + tick
# thread + commit), and ReplicaServer (m4b: one Raft replica of a
# replicated master, built on fleet_raft).
# fleetmaster is a thin main over it, so tests exercise the same code the
# binary runs.
add_library(fleet_master_lib master_core.cpp master_server.cpp failure_detector.cpp
                             master_state.cpp wal.cpp replica_server.cpp)
target_include_directories(fleet_master_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fleet_master_lib PUBLIC fleet_core fleet_proto fleet_common fleet_io fleet_raft
                                              Threads::Threads
                                       PRIVATE fleet_flags)

add_executable(fleetmaster fleetmaster.cpp)
target_link_libraries(fleetmaster PRIVATE fleet_master_lib fleet_flags)

# walbench (m4): sync latency, group commit vs per-record sync, replay vs
# snapshot load; run by bench/wal_commit.sh.
add_executable(walbench walbench.cpp)
target_link_libraries(walbench PRIVATE fleet_master_lib fleet_flags)

# replicabench (m4b): per-call latency of a write and a read, m4 single
# master against the leader of 3 Raft replicas; run by
# bench/leader_failover.sh.
add_executable(replicabench replicabench.cpp)
target_link_libraries(replicabench PRIVATE fleet_master_lib fleet_flags)

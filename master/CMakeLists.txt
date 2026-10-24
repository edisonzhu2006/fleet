# fleet_master_lib: MasterCore (logic, no threads, no network), its
# FailureDetector (m3), MasterState (durable state + apply, m4), Storage
# (write-ahead log + snapshot, m4), and MasterServer (gRPC service + tick
# thread + commit).
# fleetmaster is a thin main over it, so tests exercise the same code the
# binary runs.
add_library(fleet_master_lib master_core.cpp master_server.cpp failure_detector.cpp
                             master_state.cpp wal.cpp)
target_include_directories(fleet_master_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fleet_master_lib PUBLIC fleet_core fleet_proto fleet_common Threads::Threads
                                       PRIVATE fleet_flags)

add_executable(fleetmaster fleetmaster.cpp)
target_link_libraries(fleetmaster PRIVATE fleet_master_lib fleet_flags)

# walbench (m4): sync latency, group commit vs per-record sync, replay vs
# snapshot load; run by bench/wal_commit.sh.
add_executable(walbench walbench.cpp)
target_link_libraries(walbench PRIVATE fleet_master_lib fleet_flags)

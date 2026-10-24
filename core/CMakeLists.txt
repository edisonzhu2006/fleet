# fleet_core: the data model, ClusterState and the Scheduler. A pure,
# single-threaded library with no I/O, shared by the simulator (m1), the
# master (m2+) and the unit tests, so all three run the same object code.
add_library(fleet_core
  cluster_state.cpp
  scheduler.cpp
)
target_include_directories(fleet_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fleet_core PRIVATE fleet_flags)

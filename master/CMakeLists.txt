# fleet_master_lib: MasterCore (logic, no threads, no network) and
# MasterServer (gRPC service + scheduler thread). fleetmaster is a thin
# main over it, so tests exercise the same code the binary runs.
add_library(fleet_master_lib master_core.cpp master_server.cpp)
target_include_directories(fleet_master_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fleet_master_lib PUBLIC fleet_core fleet_proto fleet_common Threads::Threads
                                       PRIVATE fleet_flags)

add_executable(fleetmaster fleetmaster.cpp)
target_link_libraries(fleetmaster PRIVATE fleet_master_lib fleet_flags)

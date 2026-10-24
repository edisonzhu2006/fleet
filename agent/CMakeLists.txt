# fleet_agent_lib: the Agent (register/heartbeat/reconcile loop) and the
# Runtime interface with its ProcessRuntime implementation. fleetlet is a
# thin main over it; the integration tests run Agents in-process.
add_library(fleet_agent_lib agent.cpp process_runtime.cpp)
target_include_directories(fleet_agent_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(fleet_agent_lib PUBLIC fleet_core fleet_proto fleet_common Threads::Threads
                                      PRIVATE fleet_flags)

add_executable(fleetlet fleetlet.cpp)
target_link_libraries(fleetlet PRIVATE fleet_agent_lib fleet_flags)

# fleetctl: the CLI. Talks to fleetmaster only through the generated gRPC
# client, so it needs fleet_proto, plus fleet_common's header-only
# leader-redirect helpers (m4b).
add_executable(fleetctl fleetctl.cpp)
target_link_libraries(fleetctl PRIVATE fleet_proto fleet_common fleet_flags)

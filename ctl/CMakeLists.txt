# fleetctl: the CLI. Talks to fleetmaster only through the generated gRPC
# client, so it needs fleet_proto and nothing else.
add_executable(fleetctl fleetctl.cpp)
target_link_libraries(fleetctl PRIVATE fleet_proto fleet_flags)

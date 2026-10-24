# fleetsim: in-process simulated cluster. Drives fleet_core with a seeded
# synthetic workload so placement quality and decision latency can be
# measured without real processes or a network.
add_executable(fleetsim fleetsim.cpp)
target_link_libraries(fleetsim PRIVATE fleet_core fleet_flags)

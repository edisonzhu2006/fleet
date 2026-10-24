# fleet_common: small header-only helpers shared by the binaries (shutdown
# signal handling, timestamped log lines). INTERFACE because there is
# nothing to compile.
add_library(fleet_common INTERFACE)
target_include_directories(fleet_common INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

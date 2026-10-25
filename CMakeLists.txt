cmake_minimum_required(VERSION 3.24)
project(fleet LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# gRPC and Protobuf (needed from m2 on) come prebuilt from anaconda on
# Edison's Mac. Point find_package at it, and bake its lib dir into the
# build rpath so test binaries find libgrpc++ etc. without DYLD_LIBRARY_PATH,
# but only when the caller did not choose a prefix themselves, so CI and
# other machines keep their own toolchains.
if(EXISTS "$ENV{HOME}/anaconda3/lib/cmake/grpc")
  if(NOT CMAKE_PREFIX_PATH)
    set(CMAKE_PREFIX_PATH "$ENV{HOME}/anaconda3" CACHE STRING "" FORCE)
  endif()
  if(NOT CMAKE_BUILD_RPATH)
    set(CMAKE_BUILD_RPATH "$ENV{HOME}/anaconda3/lib" CACHE STRING "" FORCE)
  endif()
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# FLEET_SANITIZE: none | address | thread
set(FLEET_SANITIZE "none" CACHE STRING "Sanitizer to enable")
option(FLEET_BUILD_TESTS "Build unit tests (fetches GoogleTest)" ON)

# Project warning flags go on project targets only (fleet_flags), so the
# fetched GoogleTest is not compiled with them.
add_library(fleet_flags INTERFACE)
target_compile_options(fleet_flags INTERFACE -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion)

if(FLEET_SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=address,undefined)
elseif(FLEET_SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=thread)
endif()

find_package(Threads REQUIRED)

# Static libraries that depend on each other (fleet_master_lib and
# fleet_agent_lib both need fleet_core and fleet_proto) appear more than
# once on a link line by design; CMake repeats them so single-pass linkers
# resolve every symbol. Apple's ld warns about the repeats, so the warning
# is turned off there rather than reordering libraries by hand.
if(APPLE)
  add_link_options(LINKER:-no_warn_duplicate_libraries)
endif()

# gRPC and Protobuf (m2 on). Both in CONFIG mode: the anaconda builds ship
# CMake package files, which give imported targets (protobuf::protoc,
# gRPC::grpc_cpp_plugin, gRPC::grpc++) instead of variables. Protobuf falls
# back to CMake's own FindProtobuf module for distributions (Debian/Ubuntu)
# that package protobuf without its CMake config.
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()
find_package(gRPC CONFIG REQUIRED)
message(STATUS "fleet: gRPC ${gRPC_VERSION}, Protobuf ${Protobuf_VERSION}")

if(FLEET_BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  enable_testing()
  include(GoogleTest)
endif()

# Each component owns its CMakeLists.txt. Libraries: fleet_core (model +
# scheduler, pure), fleet_proto (gRPC/Protobuf stubs), fleet_io (frames and
# fsync, m4b), fleet_raft (Raft consensus, m4b), fleet_master_lib (master
# logic + gRPC servers, single and replicated), fleet_agent_lib (agent +
# runtimes). Executables: fleetmaster, fleetlet, fleetctl, fleetsim. Tests:
# tests/*.cpp -> fleet_tests.
foreach(dir common core proto raft master agent ctl sim)
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/CMakeLists.txt)
    add_subdirectory(${dir})
  endif()
endforeach()

if(FLEET_BUILD_TESTS)
  file(GLOB FLEET_TEST_SOURCES CONFIGURE_DEPENDS tests/*.cpp)
  if(FLEET_TEST_SOURCES)
    add_executable(fleet_tests ${FLEET_TEST_SOURCES})
    target_link_libraries(fleet_tests PRIVATE fleet_flags GTest::gtest_main Threads::Threads)
    foreach(lib fleet_core fleet_proto fleet_io fleet_raft fleet_master_lib fleet_agent_lib)
      if(TARGET ${lib})
        target_link_libraries(fleet_tests PRIVATE ${lib})
      endif()
    endforeach()
    target_include_directories(fleet_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    # Under ASan, turn off exactly one check: container-overflow. Protobuf's
    # RepeatedField marks its unused capacity poisoned through ASan
    # annotations in its *headers* (compiled here, instrumented), but the
    # prebuilt libprotobuf that fills the field while parsing a gRPC reply
    # is not instrumented and never updates those marks, so our first read
    # of a parsed repeated field is reported as an overflow. This is the
    # mixed-instrumentation false positive the ASan docs describe; heap
    # overflow, use-after-free and the rest stay on.
    if(FLEET_SANITIZE STREQUAL "address")
      gtest_discover_tests(fleet_tests PROPERTIES ENVIRONMENT "ASAN_OPTIONS=detect_container_overflow=0")
    else()
      gtest_discover_tests(fleet_tests)
    endif()
  endif()
endif()

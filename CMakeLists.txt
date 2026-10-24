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
# scheduler, pure), fleet_proto (gRPC/Protobuf stubs, m2). Executables:
# fleetmaster, fleetlet, fleetctl, fleetsim. Tests: tests/*.cpp -> fleet_tests.
foreach(dir core proto master agent ctl sim)
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/CMakeLists.txt)
    add_subdirectory(${dir})
  endif()
endforeach()

if(FLEET_BUILD_TESTS)
  file(GLOB FLEET_TEST_SOURCES CONFIGURE_DEPENDS tests/*.cpp)
  if(FLEET_TEST_SOURCES)
    add_executable(fleet_tests ${FLEET_TEST_SOURCES})
    target_link_libraries(fleet_tests PRIVATE fleet_flags GTest::gtest_main Threads::Threads)
    foreach(lib fleet_core fleet_proto fleet_master_lib fleet_agent_lib)
      if(TARGET ${lib})
        target_link_libraries(fleet_tests PRIVATE ${lib})
      endif()
    endforeach()
    target_include_directories(fleet_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    gtest_discover_tests(fleet_tests)
  endif()
endif()

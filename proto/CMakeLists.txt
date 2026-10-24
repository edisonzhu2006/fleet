# fleet_proto: C++ messages and gRPC stubs generated from fleet.proto.
#
# Generated with an explicit add_custom_command rather than
# protobuf_generate(): in testing, protobuf_generate's grpc mode did not
# put the .grpc.pb.* outputs where its own target expected them. The
# explicit command is also easier to read: one protoc run, two plugins,
# four output files in the build tree.
#
# Generated code is not project code, so this target does not link
# fleet_flags (protoc's output does not pass -Wconversion), and the
# include dir is SYSTEM so those headers do not warn in our sources either.

set(FLEET_PROTO_SRC ${CMAKE_CURRENT_SOURCE_DIR}/fleet.proto)
set(FLEET_PROTO_OUT ${CMAKE_CURRENT_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${FLEET_PROTO_OUT})
set(FLEET_PROTO_GENERATED
  ${FLEET_PROTO_OUT}/fleet.pb.cc
  ${FLEET_PROTO_OUT}/fleet.pb.h
  ${FLEET_PROTO_OUT}/fleet.grpc.pb.cc
  ${FLEET_PROTO_OUT}/fleet.grpc.pb.h)

add_custom_command(
  OUTPUT ${FLEET_PROTO_GENERATED}
  COMMAND $<TARGET_FILE:protobuf::protoc>
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
    --cpp_out=${FLEET_PROTO_OUT}
    --grpc_out=${FLEET_PROTO_OUT}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
    ${FLEET_PROTO_SRC}
  DEPENDS ${FLEET_PROTO_SRC} protobuf::protoc gRPC::grpc_cpp_plugin
  COMMENT "protoc: generating fleet.proto messages and gRPC stubs"
  VERBATIM)

add_library(fleet_proto ${FLEET_PROTO_GENERATED})
target_include_directories(fleet_proto SYSTEM PUBLIC ${FLEET_PROTO_OUT})
target_link_libraries(fleet_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

cmake_minimum_required(VERSION 3.10)

project(file_processor_server)

set(CMAKE_CXX_STANDARD 17)

# gRPC and Protobuf
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/generated
    ${Protobuf_INCLUDE_DIRS}
    ${gRPC_INCLUDE_DIRS}
)

# Fontes
set(SOURCE_FILES
    server.cc
    file_processor_service_impl.cc
    generated/file_processor.pb.cc
    generated/file_processor.grpc.pb.cc
)

add_executable(file_processor_server ${SOURCE_FILES})

# Links
target_link_libraries(file_processor_server
    gRPC::grpc++
    ${Protobuf_LIBRARIES}
)

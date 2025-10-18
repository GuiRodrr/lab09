cmake_minimum_required(VERSION 3.10)
project(file_processor_server)

set(CMAKE_CXX_STANDARD 17)

# Encontrar pacotes necessários
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# Arquivos gerados do Protobuf
set(PROTO_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/file_processor.pb.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/file_processor.grpc.pb.cc
)

message(STATUS "Using Protobuf ${Protobuf_VERSION}")

# Incluir diretórios
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${Protobuf_INCLUDE_DIRS})

# Executável do servidor
add_executable(server 
    server.cpp
    ${PROTO_SRC}
)

# Linkar bibliotecas
target_link_libraries(server
    gRPC::grpc++
    gRPC::grpc
    ${Protobuf_LIBRARIES}
)
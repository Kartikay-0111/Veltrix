cmake_minimum_required(VERSION 3.20)
project(veltrix_bot_fleet CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── Compiler flags ────────────────────────────────────────────────────────────
add_compile_options(
    -O3                   # full optimisation
    -march=native         # use all CPU features available on this machine
    -Wall
    -Wextra
    -pthread
)

# ── Find dependencies ─────────────────────────────────────────────────────────
find_package(Boost 1.78 REQUIRED COMPONENTS system)
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)

# Find gRPC via pkg-config (works on Ubuntu 24.04 with libgrpc++-dev)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPC REQUIRED grpc++ grpc)

# ── liburing (io_uring userspace library) ─────────────────────────────────────
find_library(URING_LIB NAMES uring)
if(NOT URING_LIB)
    message(FATAL_ERROR "liburing not found. Install liburing-dev or provide -luring")
endif()

# ── Protobuf/gRPC code generation from shared proto ──────────────────────────
# The proto file lives at veltrix/proto/telemetry.proto (one level up from bot-fleet/)
set(PROTO_DIR "${CMAKE_SOURCE_DIR}/../proto")
set(PROTO_FILE "${PROTO_DIR}/telemetry.proto")
set(PROTO_GEN_DIR "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

# Find the gRPC C++ plugin
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)
if(NOT GRPC_CPP_PLUGIN)
    # Fallback path for Ubuntu packages
    find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin PATHS /usr/bin)
endif()

add_custom_command(
    OUTPUT
        "${PROTO_GEN_DIR}/telemetry.pb.cc"
        "${PROTO_GEN_DIR}/telemetry.pb.h"
        "${PROTO_GEN_DIR}/telemetry.grpc.pb.cc"
        "${PROTO_GEN_DIR}/telemetry.grpc.pb.h"
    COMMAND protobuf::protoc
        --proto_path=${PROTO_DIR}
        --cpp_out=${PROTO_GEN_DIR}
        --grpc_out=${PROTO_GEN_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
        ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating C++ protobuf/gRPC stubs from telemetry.proto"
)

# ── Source files ──────────────────────────────────────────────────────────────
set(SOURCES
    src/main.cpp
    src/fleet_commander.cpp
    src/thread_worker.cpp
    src/rest_bot.cpp
    src/grpc_telemetry.cpp
    # Generated protobuf sources
    "${PROTO_GEN_DIR}/telemetry.pb.cc"
    "${PROTO_GEN_DIR}/telemetry.grpc.pb.cc"
)

add_executable(bot_fleet ${SOURCES})

target_include_directories(bot_fleet PRIVATE
    include
    ${PROTO_GEN_DIR}         # generated pb.h / grpc.pb.h
    ${GRPC_INCLUDE_DIRS}
    ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(bot_fleet
    PRIVATE
    Boost::system
    Threads::Threads
    ${URING_LIB}             # -luring
    ${GRPC_LIBRARIES}        # gRPC++
    protobuf::libprotobuf    # protobuf runtime
)

# ── Tell Boost.Asio to use io_uring instead of epoll ─────────────────────────
target_compile_definitions(bot_fleet PRIVATE
    BOOST_ASIO_HAS_IO_URING=1
    BOOST_ASIO_DISABLE_EPOLL=1
)
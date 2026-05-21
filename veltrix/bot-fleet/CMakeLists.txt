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

# ── liburing (io_uring userspace library) ─────────────────────────────────────
find_library(URING_LIB uring REQUIRED)

# ── librdkafka (Redpanda/Kafka producer) ──────────────────────────────────────
find_library(RDKAFKA_LIB rdkafka REQUIRED)

# ── Source files ──────────────────────────────────────────────────────────────
set(SOURCES
    src/main.cpp
    src/fleet_commander.cpp
    src/thread_worker.cpp
    src/rest_bot.cpp
    src/telemetry.cpp
)

add_executable(bot_fleet ${SOURCES})

target_include_directories(bot_fleet PRIVATE include)

target_link_libraries(bot_fleet
    PRIVATE
    Boost::system
    Threads::Threads
    ${URING_LIB}      # -luring
    ${RDKAFKA_LIB}    # -lrdkafka
)

# ── Tell Boost.Asio to use io_uring instead of epoll ─────────────────────────
target_compile_definitions(bot_fleet PRIVATE
    BOOST_ASIO_HAS_IO_URING=1
    BOOST_ASIO_DISABLE_EPOLL=1
)

cmake_minimum_required(VERSION 3.20)
project(veltrix_artifact_checker CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-O3 -march=native -Wall -Wextra -pthread)

# ── pkg-config: correct way to find rdkafka++ and libpqxx ────────────────────
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA_CPP REQUIRED rdkafka++)
pkg_check_modules(LIBPQXX     REQUIRED libpqxx)
find_package(Threads REQUIRED)

# ── Source files via GLOB (avoids path resolution issues in Docker) ────────────
file(GLOB SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
message(STATUS "Sources found: ${SOURCES}")

if(NOT SOURCES)
    message(FATAL_ERROR "No .cpp files found in ${CMAKE_CURRENT_SOURCE_DIR}/src/")
endif()

add_executable(artifact_checker ${SOURCES})

target_include_directories(artifact_checker PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${RDKAFKA_CPP_INCLUDE_DIRS}
    ${LIBPQXX_INCLUDE_DIRS}
)

target_link_libraries(artifact_checker PRIVATE
    Threads::Threads
    ${RDKAFKA_CPP_LIBRARIES}
    ${LIBPQXX_LIBRARIES}
)

target_link_options(artifact_checker PRIVATE
    ${RDKAFKA_CPP_LDFLAGS}
    ${LIBPQXX_LDFLAGS}
)
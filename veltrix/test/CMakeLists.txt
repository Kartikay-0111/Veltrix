cmake_minimum_required(VERSION 3.16)

project(orderbook_submission LANGUAGES CXX)

add_executable(server main.cpp)

target_compile_features(server PRIVATE cxx_std_20)

find_package(Threads REQUIRED)
target_link_libraries(server PRIVATE Threads::Threads)

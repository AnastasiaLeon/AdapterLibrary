cmake_minimum_required(VERSION 3.12)

project(
    labwork8
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 23)

include_directories(lib)

add_subdirectory(bin)

enable_testing()
add_subdirectory(tests)
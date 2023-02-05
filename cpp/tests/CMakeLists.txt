#cmake_minimum_required(VERSION 3.22)

#set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../bin)
#set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/../bin)
#set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/../bin)

#find_package(Python 3.10 REQUIRED COMPONENTS Interpreter Development NumPy)
#find_package(pybind11 REQUIRED)
#include_directories(${Python_INCLUDE_DIRS})
#message(${Python_INCLUDE_DIRS})
#find_library(XDBC_LIB xdbc HINTS /usr/local/lib/)

set(CMAKE_VERBOSE_MAKEFILE ON)

add_executable(test_xdbc src/test_xdbc.cpp)

target_link_libraries(test_xdbc PUBLIC pybind11::headers xdbc xtensor-python Python3::NumPy )
target_include_directories(test_xdbc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/cpp/include pybind11::headers)

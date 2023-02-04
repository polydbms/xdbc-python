#cmake_minimum_required(VERSION 3.21)

#project(xdbc)
#set(CMAKE_VERBOSE_MAKEFILE ON)
#set(CMAKE_CXX_STANDARD 17)
#set(CMAKE_CXX_STANDARD_REQUIRED ON)
#set(CMAKE_CXX_EXTENSIONS OFF)


# Include dir
#include_directories(/usr/local/include /usr/include/python3.10)
#find_package(Python3 REQUIRED COMPONENTS Interpreter Development NumPy)
#find_package(pybind11 REQUIRED)
include_directories(${Python_INCLUDE_DIRS})

# Src
AUX_SOURCE_DIRECTORY(src SRC_FILES)

# Headers
set(PROJECT_SOURCE_DIR "src")
set(PROJECT_INCLUDE_DIR "include")

# Source files
set(SOURCE_FILES
#        ${PROJECT_INCLUDE_DIR}/xdbc.h
        ${PROJECT_SOURCE_DIR}/xdbc.cpp
        )

# Add library
add_library(xdbc SHARED ${SOURCE_FILES})

# Include directories
target_include_directories(xdbc PRIVATE include)

target_link_libraries(xdbc PUBLIC -lpq pybind11::module Python3::NumPy ${Python3_LIBRARIES})


# Install
install(TARGETS xdbc DESTINATION lib)

# Install the headers
#install(FILES include/xdbc.h DESTINATION xdbc)

# Create base directory
install(FILES include/xdbc.h DESTINATION include/xdbc)

add_subdirectory(./tests)
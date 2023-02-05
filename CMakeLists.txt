cmake_minimum_required(VERSION 3.22)

project(xdbc)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()

set(CMAKE_CXX_FLAGS "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")


include_directories("${CMAKE_SOURCE_DIR}/cpp/include")
include_directories("${CMAKE_SOURCE_DIR}/python")

#file (GLOB SOURCE_FILES "cpp/src/*.cpp")
#file (GLOB HEADER_FILES "cpp/include/*.h")
file(GLOB PYTHON_FILES "python/*.cpp" "python/*.hpp")

# Set up such that XCode organizes the files
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCE_FILES} ${HEADER_FILES} ${PYTHON_FILES})

find_package(Python3 REQUIRED COMPONENTS Interpreter Development NumPy)
find_package(pybind11 REQUIRED)
find_package(xtensor REQUIRED)
find_package(xtensor-python REQUIRED)

pybind11_add_module(pyxdbc
        #${SOURCE_FILES}
        #${HEADER_FILES}
        ${PYTHON_FILES}
        )
add_subdirectory(./cpp)

target_include_directories(pyxdbc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/cpp/include)
target_link_libraries(pyxdbc PUBLIC xdbc pybind11::module xtensor-python Python3::NumPy -lpq)

#set(PYTHON_LIBRARY_DIR "/home/harry/miniconda3/lib/python3.9/site-packages")
#set(PYTHON_EXECUTABLE "/home/harry/miniconda3/bin/python3")

set(PYTHON_LIBRARY_DIR "/root/miniconda3/envs/python39/lib/python3.9/site-packages")
set(PYTHON_EXECUTABLE "/root/miniconda3/envs/python39/bin/python3")
set(PYTHON_LIBRARIES "/root/miniconda3/envs/python39/lib/libpython3.9.so")

install(TARGETS pyxdbc
        COMPONENT python
        LIBRARY DESTINATION "${PYTHON_LIBRARY_DIR}"
        )
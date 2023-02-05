cmake_minimum_required(VERSION 3.22)

project(xdbc)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()

set(CMAKE_CXX_FLAGS "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")


include_directories("${CMAKE_SOURCE_DIR}/cpp/include")
include_directories("${CMAKE_SOURCE_DIR}/python")

#file (GLOB SOURCE_FILES "cpp/src/*.cpp")
#file (GLOB HEADER_FILES "cpp/include/*.h")
file(GLOB PYTHON_FILES "python/*.cpp" "python/*.hpp")

# Set up such that XCode organizes the files
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCE_FILES} ${HEADER_FILES} ${PYTHON_FILES})

find_package(Python3 3.9 REQUIRED COMPONENTS Interpreter Development NumPy)
find_package(pybind11 REQUIRED)
find_package(xtensor REQUIRED)
find_package(xtensor-python REQUIRED)

message(STATUS pybind11::headers)


pybind11_add_module(pyxdbc
        #${SOURCE_FILES}
        #${HEADER_FILES}
        ${PYTHON_FILES}
        )
add_subdirectory(./cpp)

target_include_directories(pyxdbc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/cpp/include pybind11::headers)
target_link_libraries(pyxdbc PUBLIC xdbc xtensor-python Python3::NumPy -lpq pybind11::headers)

set(PYTHON_LIBRARY_DIR "/home/harry/miniconda3/lib/python3.9/site-packages")
set(PYTHON_EXECUTABLE "/home/harry/miniconda3/bin/python3")

install(TARGETS pyxdbc
        COMPONENT python
        LIBRARY DESTINATION "${PYTHON_LIBRARY_DIR}"
        )
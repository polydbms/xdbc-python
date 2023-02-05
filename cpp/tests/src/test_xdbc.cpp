#include <xdbc.h>
#include <iostream>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/pytypes.h>
#include <pybind11/pybind11.h>
#include <pybind11/pybind11.h>
#include <xtensor/xmath.hpp>              // xtensor import for the C++ universal functions
#define FORCE_IMPORT_ARRAY
#include <xtensor-python/pyarray.hpp>     // Numpy bindings


namespace py = pybind11;

int main() {

    xdbc::XClient c("Test Client");

    std::cout << "Made XClient called: " << c.get_name() << std::endl;

    pybind11::list x = c.load("lalala");
    //c.receive();

    std::cout << x.empty() << std::endl;

    return 0;
}
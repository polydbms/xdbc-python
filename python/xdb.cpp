#include <numeric>
#include <pybind11/pybind11.h>
#include <xtensor/xmath.hpp>              // xtensor import for the C++ universal functions
#define FORCE_IMPORT_ARRAY
#include <xtensor-python/pyarray.hpp>     // Numpy bindings

namespace py = pybind11;



void init_xdbc(py::module &);

namespace x {

    PYBIND11_MODULE(xdbc, m) {
        xt::import_numpy();
        m.doc() = "XDB library";

        init_xdbc(m);
    }
}
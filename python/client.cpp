#include "../cpp/include/xdbc.h"


#include <pybind11/stl.h>

#include <pybind11/pybind11.h>

namespace py = pybind11;


void init_xdbc(py::module &m) {

    py::class_<xdbc::XClient>(m, "XClient")
            .def(py::init<std::string>(), py::arg("name"))
            .def("get_name",
                 py::overload_cast<>(&xdbc::XClient::get_name, py::const_))
            .def("load",
                 py::overload_cast<const std::string>(&xdbc::XClient::load),
                 py::arg("table"),
                 py::return_value_policy::move);
}


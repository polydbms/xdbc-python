#ifndef XDBC_LIBRARY_H
#define XDBC_LIBRARY_H

#include <string>
#include <pybind11/numpy.h>
#include <boost/asio.hpp>

#define TOTAL_TUPLES 10000000
#define BUFFER_SIZE 1000
#define BUFFERPOOL_SIZE 1000
#define TUPLE_SIZE 48
#define SLEEP_TIME 10ms


namespace py = pybind11;
namespace xdbc {

    struct shortLineitem {
        int l_orderkey;
        int l_partkey;
        int l_suppkey;
        int l_linenumber;
        double l_quantity;
        double l_extendedprice;
        double l_discount;
        double l_tax;
    };


    class XClient {
    private:
        std::string _name;

        int _flagArray[BUFFERPOOL_SIZE];
        std::vector<std::array<shortLineitem, BUFFER_SIZE>> _bufferPool;


    public:
        XClient(std::string name);

        std::string get_name() const;

        void receive();

        py::list load(std::string);
    };


}

#endif //XDBC_LIBRARY_H

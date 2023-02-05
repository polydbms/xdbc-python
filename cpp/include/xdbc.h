#ifndef XDBC_LIBRARY_H
#define XDBC_LIBRARY_H

#include <string>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/pytypes.h>
#include <boost/asio.hpp>
#include <iostream>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/pytypes.h>
#include <boost/asio.hpp>
#include <thread>
#include <chrono>
#define TOTAL_TUPLES 10000000
#define BUFFER_SIZE 1000
#define BUFFERPOOL_SIZE 1000
#define TUPLE_SIZE 48
#define SLEEP_TIME std::chrono::milliseconds(10)


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


    void printSl(shortLineitem *t) {
        using namespace std;
        cout << t->l_orderkey << " | "
             << t->l_partkey << " | "
             << t->l_suppkey << " | "
             << t->l_linenumber << " | "
             << t->l_quantity << " | "
             << t->l_extendedprice << " | "
             << t->l_discount << " | "
             << t->l_tax
             << endl;
    }

    class XClient {
    private:
        std::string _name;

        int _flagArray[BUFFERPOOL_SIZE];
        std::vector<std::array<shortLineitem, BUFFER_SIZE>> _bufferPool;


    public:
        XClient(std::string name);

        std::string get_name() const;

        void receive();

    py::object load(std::string table) {
        std::cout << "Fetching table:" << table << std::endl;

        std::thread t1(&XClient::receive, this);
        //receive();
        std::cout << "Fetched. now deserializing:" << std::endl;
        int *ints = new int[TOTAL_TUPLES * 4];
        double *doubles = new double[TOTAL_TUPLES * 4];

        int bpi = 0;
        int tupleCnt = 0;
        while (tupleCnt < TOTAL_TUPLES) {

            while (_flagArray[bpi])
                std::this_thread::sleep_for(SLEEP_TIME);

            //const char *data = boost::asio::buffer_cast<const char *>(_bufferpool[bpi].data());
            //shortLineitem *t = reinterpret_cast<shortLineitem *>(&_bufferpool + i * TUPLE_SIZE);
            //printSl(t);
            //const char *data = reinterpret_cast<const char *>(&_bufferPool[bpi]);

            for (auto sl: _bufferPool[bpi]) {
                //TODO: dirty fix
                printSl(&sl);
                if (sl.l_orderkey != -1) {
                    memcpy(&ints[tupleCnt * 4], &sl, 4 * sizeof(int));
                    memcpy(&doubles[tupleCnt * 4], &sl.l_quantity, 4 * sizeof(double));

                    tupleCnt++;
                }
                /*for (int j = 0; j < 4; j++) {
                    memcpy(&ints[i * 4 + j], &sl + i * TUPLE_SIZE + j * sizeof(int), sizeof(int));
                    memcpy(&doubles[i * 4 + j], &sl + i * TUPLE_SIZE + 16 + j * sizeof(double),
                           sizeof(double));
                }*/
            }

            _flagArray[bpi] = 1;
            bpi++;
            if (bpi == BUFFERPOOL_SIZE)
                bpi = 0;


        }


        // Create a Python object that will free the allocated
        // memory when destroyed:
        py::capsule free_when_done(ints, [](void *f) {
            int *foo = reinterpret_cast<int *>(f);
            std::cerr << "Element [0] = " << foo[0] << "\n";
            std::cerr << "freeing memory @ " << f << "\n";
            delete[] foo;
        });

        auto list = py::list();


        auto arrayints = py::array_t<int>(
                {TOTAL_TUPLES, 4}, // shape
                {4 * 4, 4}, // C-style contiguous strides for double
                ints, // the data pointer
                free_when_done); // numpy array references this parent

        auto arraydoubles = py::array_t<double>(
                {TOTAL_TUPLES, 4}, // shape
                {4 * 8, 8}, // C-style contiguous strides for double
                doubles, // the data pointer
                free_when_done); // numpy array references this parent
        list.append(arrayints);
        list.append(arraydoubles);

        t1.join();

        return list;
    }
    };


}

#endif //XDBC_LIBRARY_H

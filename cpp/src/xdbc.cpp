#include <xdbc.h>

#include <iostream>
#include <pybind11/numpy.h>
#include <boost/asio.hpp>
#include <thread>
#include <chrono>

using namespace std;
namespace py = pybind11;
namespace xdbc {

    XClient::XClient(std::string name) : _name(name), _bufferPool(), _flagArray() {

        cout << _name << endl;

        cout << "BUFFERPOOL SIZE: " << BUFFERPOOL_SIZE << endl;
        _bufferPool.resize(BUFFERPOOL_SIZE);
        for (int i = 0; i < BUFFERPOOL_SIZE; i++) {
            _flagArray[i] = 1;

        }

    }

    std::string XClient::get_name() const {
        return _name;
    }

    void printSl(shortLineitem *t) {
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


    void XClient::receive() {
        using namespace boost::asio;
        using ip::tcp;

        boost::asio::io_service io_service;
        //socket creation
        ip::tcp::socket socket(io_service);
        //connection
        socket.connect(tcp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), 1234));

        const std::string msg = "Give!\n";
        boost::system::error_code error;
        boost::asio::write(socket, boost::asio::buffer(msg), error);

        int bpi = 0;
        int buffers = 0;

        cout << "Starting" << endl;
        while (buffers < TOTAL_TUPLES / BUFFER_SIZE) {
            //cout << "Reading" << endl;

            while (_flagArray[bpi] == 0) {
                std::this_thread::sleep_for(SLEEP_TIME);
                std::cout << "Sleeping at " << bpi << std::endl;
            }

            // getting response from server

            boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi]),
                              boost::asio::transfer_exactly(BUFFER_SIZE * TUPLE_SIZE), error);

            /*for (auto x: _bufferPool[bpi]) {

                printSl(&x);
            }*/

            _flagArray[bpi] = 0;
            bpi++;
            if (bpi == BUFFERPOOL_SIZE)
                bpi = 0;

            buffers++;

            /* cout << "bpi: " << bpi << endl;
             cout << "flagidx " << (bpi / (BUFFER_SIZE * TUPLE_SIZE)) << endl;*/



        }


    }


    void XClient::load(std::string table) {
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

        //return list;
    }
}

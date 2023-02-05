#include <xdbc.h>

#include <iostream>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/pytypes.h>
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


    
}

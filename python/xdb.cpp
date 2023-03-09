#include <xclient.h>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <iostream>
#include <thread>

using namespace std;
namespace py = pybind11;

py::list load(std::string table, int total_tuples) {

    int *ints = new int[total_tuples * 4];
    double *doubles = new double[total_tuples * 4];

    xdbc::XClient c("PyXDBC Client");

    cout << "Constructed XClient called: " << c.get_name() << endl;

    thread t1 = c.startReceiving(table);

    int cnt = 0;
    long totalcnt = 0;

    cout << "Started receive" << endl;

    auto start = std::chrono::steady_clock::now();

    int buffsRead = 0;
    while (c.hasUnread()) {
        //cout << "Started iteration" << endl;
        xdbc::buffWithId curBuffWithId = c.getBuffer();
        if (curBuffWithId.id >= 0) {
            for (auto sl: curBuffWithId.buff) {
                totalcnt++;

                if (sl.l_orderkey < 0) {
                    cout << "Empty tuple at buffer: " << curBuffWithId.id << " and tuple " << cnt << endl;
                    c.printSl(&sl);
                    break;
                }

                memcpy(&ints[cnt * 4], &sl, 4 * sizeof(int));
                memcpy(&doubles[cnt * 4], &sl.l_quantity, 4 * sizeof(double));
                cnt++;


            }

        } else
            cout << " found buffer with id: " << curBuffWithId.id << endl;
        buffsRead++;
        c.markBufferAsRead(curBuffWithId.id);
    }

    cout << "Total read Buffers: " << buffsRead << " and tuples:" << cnt << endl;

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
            {total_tuples, 4}, // shape
            {4 * 4, 4}, // C-style contiguous strides for double
            ints, // the data pointer
            free_when_done); // numpy array references this parent

    auto arraydoubles = py::array_t<double>(
            {total_tuples, 4}, // shape
            {4 * 8, 8}, // C-style contiguous strides for double
            doubles, // the data pointer
            free_when_done); // numpy array references this parent
    list.append(arrayints);
    list.append(arraydoubles);

    t1.join();

    return list;
};


PYBIND11_MODULE(pyxdbc, m) {

    m.doc() = "XDB library";

    m.def("load", &load);
}

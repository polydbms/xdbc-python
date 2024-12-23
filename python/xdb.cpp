#include <xclient.h>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <iostream>
#include <thread>
#include <numeric>
#include <algorithm>
#include <nlohmann/json.hpp>
#include <fstream>
#include <condition_variable>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

using namespace std;
namespace py = pybind11;

vector <xdbc::SchemaAttribute> createSchemaFromConfig(const string &configFile) {
    ifstream file(configFile);
    if (!file.is_open()) {
        cout << "failed to open schema" << configFile << endl;

    }
    nlohmann::json schemaJson;
    file >> schemaJson;

    vector <xdbc::SchemaAttribute> schema;
    for (const auto &item: schemaJson) {
        schema.emplace_back(xdbc::SchemaAttribute{
                item["name"],
                item["type"],
                item["size"]
        });
    }
    return schema;
}

std::string readJsonFileIntoString(const std::string &filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        cout << "failed to open schema" << filePath << endl;
        return "";
    }

    std::stringstream buffer;
    buffer << file.rdbuf();

    return buffer.str();
}

struct Partition {
    long startOffset;
    long endOffset;
};

typedef std::shared_ptr <customQueue<Partition>> FBQ_ptr;
typedef std::shared_ptr <customQueue<Partition>> ST_ptr;

void process_buffer(ST_ptr serqueue, FBQ_ptr queue, xdbc::XClient &c, xdbc::RuntimeEnv &env, int thread_num,
                    std::vector <std::vector<int>> &int_columns,
                    std::vector <std::vector<double>> &double_columns,
                    std::vector <std::vector<char>> &char_columns,
                    std::vector <std::vector<std::string>> &string_columns) {

    std::string stringValue;
    env.pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thread_num, "write", "start"});
    //TODO: for columnar format (2)
    size_t baseOffset = 0;
    std::vector <size_t> offsets(env.schema.size());
    for (size_t i = 0; i < env.schema.size(); ++i) {
        offsets[i] = baseOffset;
        baseOffset += env.tuples_per_buffer * env.schema[i].size;
    }

    size_t schemaSize = env.schema.size();
    std::vector <size_t> attr_sizes(env.schema.size());
    for (size_t j = 0; j < env.schema.size(); ++j) {
        attr_sizes[j] = env.schema[j].size;
    }

    long totalCnt = 0;

    auto p = queue->pop();
    auto cnt = p.startOffset;
    auto end_idx = p.endOffset;
    auto startSerTask = cnt;
    auto endSerTask = end_idx;


    spdlog::get("pyXCLIENT")->info("Thread {0}, partition [{1},{2}]", thread_num, cnt, end_idx);
    while (c.hasNext(thread_num)) {
        xdbc::buffWithId curBuffWithId = c.getBuffer(thread_num);
        if (curBuffWithId.id >= 0) {
            if (curBuffWithId.iformat == 1) {

                char *ptr = reinterpret_cast<char *>(curBuffWithId.buff);

                for (int i = 0; i < curBuffWithId.totalTuples; ++i) {
                    char *tuple_ptr = ptr + i * env.tuple_size;


                    int offset = 0;
                    int int_idx = 0;
                    int double_idx = 0;
                    int char_idx = 0;
                    int string_idx = 0;

                    for (size_t j = 0; j < env.schema.size(); ++j) {

                        if (cnt >= end_idx) {
                            //TODO: check this logic
                            /* spdlog::get("pyXCLIENT")->info(
                                     "Thread {} Sending, [{},{}] to deserializer before receiving new slice",
                                     thread_num, startSerTask, cnt);*/
                            Partition serTask{startSerTask, cnt};
                            serqueue->push(serTask);

                            spdlog::get("pyXCLIENT")->info("Thread {0}, requesting new slice", thread_num);
                            auto p = queue->pop();
                            cnt = p.startOffset;
                            end_idx = p.endOffset;
                            spdlog::get("pyXCLIENT")->info("Thread {0}, got slice [{1},{2}]", thread_num, cnt, end_idx);
                            startSerTask = cnt;
                        }

                        const auto &attr = env.schema[j];

                        switch (attr.tpe[0]) {
                            case 'I': {
                                int_columns[int_idx++][cnt] = *reinterpret_cast<int *>(tuple_ptr + offset);
                                break;
                            }
                            case 'D': {
                                double_columns[double_idx++][cnt] = *reinterpret_cast<double *>(tuple_ptr + offset);
                                break;
                            }
                            case 'C': {
                                char_columns[char_idx++][cnt] = *reinterpret_cast<char *>(tuple_ptr + offset);
                                break;

                            }
                            case 'S': {
                                stringValue.assign(tuple_ptr + offset, attr.size);
                                string_columns[string_idx++][cnt] = stringValue;
                                break;
                            }
                            default:
                                break;
                        }
                        offset += attr.size;
                    }

                    totalCnt++;
                    cnt++;
                }
                /*spdlog::get("pyXCLIENT")->info("Thread {} sending [{},{}] to deserializer",
                                               thread_num, startSerTask, cnt);*/
                Partition serTask{startSerTask, cnt};
                serqueue->push(serTask);
                startSerTask = cnt;

            }

            if (curBuffWithId.iformat == 2) {
                char *dataPtr = reinterpret_cast<char *>(curBuffWithId.buff);
                std::vector<void *> pointers(schemaSize);

                for (size_t j = 0; j < schemaSize; ++j) {
                    pointers[j] = dataPtr + offsets[j];
                }

                size_t tuplesRemaining = curBuffWithId.totalTuples;

                while (tuplesRemaining > 0) {
                    if (cnt >= end_idx) {
                        spdlog::get("pyXCLIENT")->info("Thread {0}, requesting new slice", thread_num);
                        auto p = queue->pop();
                        cnt = p.startOffset;
                        end_idx = p.endOffset;
                        spdlog::get("pyXCLIENT")->info("Thread {0}, got slice [{1},{2}]", thread_num, cnt, end_idx);
                    }

                    size_t tuplesToProcess = std::min(tuplesRemaining, static_cast<size_t>(end_idx - cnt));

                    int int_idx = 0;
                    int double_idx = 0;
                    int char_idx = 0;
                    int string_idx = 0;

                    for (size_t j = 0; j < schemaSize; ++j) {
                        const auto &attr = env.schema[j];
                        char *data_ptr = reinterpret_cast<char *>(pointers[j]) +
                                         (curBuffWithId.totalTuples - tuplesRemaining) * attr_sizes[j];

                        switch (attr.tpe[0]) {
                            case 'I': {
                                size_t bytesToCopy = tuplesToProcess * sizeof(int);
                                memcpy(&int_columns[int_idx++][cnt], data_ptr, bytesToCopy);
                                break;
                            }
                            case 'D': {
                                size_t bytesToCopy = tuplesToProcess * sizeof(double);
                                memcpy(&double_columns[double_idx++][cnt], data_ptr, bytesToCopy);
                                break;
                            }
                            case 'C': {
                                size_t bytesToCopy = tuplesToProcess * sizeof(char);
                                memcpy(&char_columns[char_idx++][cnt], data_ptr, bytesToCopy);
                                break;
                            }
                            case 'S': {
                                for (size_t i = 0; i < tuplesToProcess; ++i) {
                                    string_columns[string_idx][cnt + i].assign(data_ptr + i * attr.size, attr.size);
                                }
                                string_idx++;
                                break;
                            }
                            default:
                                break;
                        }
                    }

                    Partition serTask;
                    serTask.startOffset = cnt;
                    serTask.endOffset = cnt + tuplesToProcess;
                    serqueue->push(serTask);

                    tuplesRemaining -= tuplesToProcess;
                    cnt += tuplesToProcess;
                    totalCnt += tuplesToProcess;
                }
            }


        } else {
            spdlog::get("pyXCLIENT")->info("Thread {0}, found buffer with id {1}", thread_num, curBuffWithId.id);
        }
        c.markBufferAsRead(curBuffWithId.id);
        env.pts->push(
                xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thread_num, "write", "push"});
    }

    if (cnt < end_idx)
        queue->push((Partition) {cnt, end_idx});
    spdlog::get("pyXCLIENT")->info("Thread {0}, processed tuples {1}", thread_num, totalCnt);
    spdlog::get("pyXCLIENT")->warn("Thread {0}, hasNext {1}", thread_num, c.hasNext(thread_num));
    env.pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thread_num, "write", "end"});
    Partition serTask{-1, thread_num};
    serqueue->push(serTask);
}

void serializeStrings(ST_ptr serqueue, std::vector <std::vector<std::string>> &string_columns,
                      std::vector <py::array> &py_arrays, int write_parallelism) {
    spdlog::get("pyXCLIENT")->info("Started string deserialization");
    int end_tasks_received = 0;
    Partition st = serqueue->pop();
    int totalDeserialized = 0;

    while (end_tasks_received < write_parallelism) {

        int py_array_idx = 0;
        for (auto &column: string_columns) {
            auto str_array_data = reinterpret_cast<py::object *>(py_arrays[py_array_idx].mutable_data());
            size_t fixed_length = column[st.startOffset].size();
            for (size_t i = st.startOffset; i < st.endOffset; ++i) {
                const char *str_data = column[i].data();
                str_array_data[i] = py::reinterpret_steal<py::object>(
                        PyUnicode_FromStringAndSize(str_data, fixed_length)
                );
                totalDeserialized++;
            }
            py_array_idx++;
        }

        st = serqueue->pop();
        if (st.startOffset == -1) {
            end_tasks_received++;
            spdlog::get("pyXCLIENT")->info("Writer {0} sent all strings end_tasks: {1}", st.endOffset, st.startOffset);
        }
    }

    for (auto &column: string_columns) {
        column.clear();
        column.shrink_to_fit();
    }
    spdlog::get("pyXCLIENT")->info("constructed strings: {}", totalDeserialized);
}


py::list load(py::dict pyEnv) {
    auto start_profiling = std::chrono::steady_clock::now();
    auto console = spdlog::stdout_color_mt("pyXCLIENT");

    int total_tuples = pyEnv["total_tuples"].cast<int>();
    xdbc::RuntimeEnv env;
    env.env_name = pyEnv["env_name"].cast<std::string>();
    env.table = pyEnv["table"].cast<std::string>();
    env.iformat = pyEnv["iformat"].cast<int>();
    env.buffer_size = pyEnv["buffer_size"].cast<int>();
    env.buffers_in_bufferpool = pyEnv["bufferpool_size"].cast<int>() / pyEnv["buffer_size"].cast<int>();
    env.sleep_time = std::chrono::milliseconds(pyEnv["sleep_time"].cast<int>());
    env.rcv_parallelism = pyEnv["rcv_parallelism"].cast<int>();
    env.write_parallelism = pyEnv["write_parallelism"].cast<int>();
    env.decomp_parallelism = pyEnv["decomp_parallelism"].cast<int>();
    env.transfer_id = pyEnv["transfer_id"].cast<long>();

    env.server_host = pyEnv["server_host"].cast<std::string>();
    env.server_port = pyEnv["server_port"].cast<std::string>();

    //create schema
    std::vector <xdbc::SchemaAttribute> schema;

    string schemaFile = "/xdbc-client/tests/schemas/" + env.table + ".json";

    schema = createSchemaFromConfig(schemaFile);
    env.schemaJSON = readJsonFileIntoString(schemaFile);
    env.schema = schema;

    env.tuple_size = std::accumulate(env.schema.begin(), env.schema.end(), 0,
                                     [](int acc, const xdbc::SchemaAttribute &attr) {
                                         return acc + attr.size;
                                     });

    env.tuples_per_buffer = env.buffer_size * 1024 / env.tuple_size;

    env.startTime = std::chrono::steady_clock::now();

    xdbc::XClient c(env);

    spdlog::get("pyXCLIENT")->info("Constructed XClient called: {0}", c.get_name());
    c.startReceiving(env.table);
    spdlog::get("pyXCLIENT")->info("Started receiving");

    auto start = std::chrono::steady_clock::now();
    int totalcnt = 0;
    int cnt = 0;
    int buffsRead = 0;

    // Create vectors for each attribute based on the schema
    std::vector <std::vector<int>> int_columns;
    std::vector <std::vector<double>> double_columns;
    std::vector <std::vector<char>> char_columns;
    std::vector <std::vector<std::string>> string_columns;

    // Create and resize vectors for each attribute
    for (const auto &attr: env.schema) {

        switch (attr.tpe[0]) {
            case 'I': {
                int_columns.emplace_back(std::vector<int>(total_tuples, -999));
                break;
            }
            case 'D': {
                double_columns.emplace_back(std::vector<double>(total_tuples));
                break;
            }
            case 'C': {
                char_columns.emplace_back(std::vector<char>(total_tuples));
                break;

            }
            case 'S': {
                string_columns.emplace_back(std::vector<std::string>(total_tuples));
                break;
            }
            default:
                break;
        }

    }

    py::list result;

    std::vector <py::array> py_string_arrays;

    for (auto &column: string_columns) {

        py::array str_array = py::array(py::dtype("O"), py::array::ShapeContainer{column.size()});

        py_string_arrays.push_back(str_array);

        result.append(str_array);
    }

    // Create a thread pool
    std::vector <std::thread> threads;
    int base_tuples_per_thread = total_tuples / env.write_parallelism;
    int extra_tuples = total_tuples % env.write_parallelism;

    FBQ_ptr q(new customQueue <Partition>);
    ST_ptr sq(new customQueue <Partition>);

    std::thread stringSerThread = std::thread(serializeStrings, std::ref(sq), std::ref(string_columns),
                                              std::ref(py_string_arrays), env.write_parallelism);

    for (int i = 0; i < env.write_parallelism; ++i) {
        int start_idx = i * base_tuples_per_thread + std::min(i, extra_tuples);
        int end_idx = start_idx + base_tuples_per_thread + (i < extra_tuples ? 1 : 0);
        q->push((Partition) {start_idx, end_idx});

        threads.emplace_back(process_buffer, std::ref(sq), std::ref(q), std::ref(c), std::ref(env), i,
                             std::ref(int_columns), std::ref(double_columns),
                             std::ref(char_columns), std::ref(string_columns));
    }


    // Wait for all threads to complete

    stringSerThread.join();
    for (auto &thread: threads) {
        thread.join();
    }

    c.finalize();

    auto end = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start_profiling).count();

    spdlog::get("pyXCLIENT")->info("Data Transfer: {0}s", total_time / 1000);
    auto start_arrays = std::chrono::high_resolution_clock::now();

    int column_index = 0;
    for (const auto &column: int_columns) {
        int value_index = 0;
        for (const auto &value: column) {
            if (value == -999) {
                std::cerr << "Unwritten int value detected in column "
                          << column_index << " at value index " << value_index << std::endl;
                break;
            }
            ++value_index;
        }
        ++column_index;
    }


    spdlog::get("pyXCLIENT")->info("starting np array construction");
    // Create numpy arrays for int columns
    for (auto &column: int_columns) {
        auto array = py::array_t<int>(
                {column.size()},  // shape
                {sizeof(int)},    // C-style contiguous strides
                column.data()
        );
        result.append(array);
    }

    spdlog::get("pyXCLIENT")->info("constructed int");
    // Create numpy arrays for double columns
    for (auto &column: double_columns) {
        auto array = py::array_t<double>(
                {column.size()},   // shape
                {sizeof(double)},  // C-style contiguous strides
                column.data()
        );
        result.append(array);
    }
    spdlog::get("pyXCLIENT")->info("constructed double");

    for (auto &column: char_columns) {
        // Create a numpy array with dtype=object
        py::array str_array = py::array(py::dtype("O"), py::array::ShapeContainer{column.size()});

        // Fill the numpy array with Python string objects
        auto str_array_data = reinterpret_cast<py::object *>(str_array.mutable_data());
        for (size_t i = 0; i < column.size(); ++i) {
            str_array_data[i] = py::reinterpret_steal<py::object>(PyUnicode_FromStringAndSize(&column[i], 1));
        }

        result.append(str_array);
        column.clear();
        column.shrink_to_fit();
    }
    spdlog::get("pyXCLIENT")->info("constructed chars");

    auto duration_arrays = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start_arrays).count();
    spdlog::get("pyXCLIENT")->info("Array creation: {0}s", duration_arrays / 1000);

    return result;
};


PYBIND11_MODULE(pyxdbc, m
) {
m.

doc() = "XDB library";

m.def("load", &load);
}

#include <xclient.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <arrow/python/pyarrow.h>
#include <arrow/python/arrow_to_pandas.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <vector>
#include <fstream>
#include "spdlog/spdlog.h"

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

py::object convertArrowTableToPandas(const std::shared_ptr<arrow::Table>& table, int64_t num_rows = -1) {
    // Ensure Arrow-Python integration is initialized

    arrow::py::import_pyarrow();

    std::shared_ptr<arrow::Table> table_to_convert = table;

    // If num_rows is specified, slice the table to limit the number of rows
    /*num_rows=1000;
    if (num_rows > 0 && num_rows < table->num_rows()) {
        table_to_convert = table->Slice(0, num_rows);
    }*/

    // Create Pandas options
    arrow::py::PandasOptions options;

    // Convert Arrow Table to Pandas DataFrame
    PyObject* pandas_df;
    //options.deduplicate_objects = true;
    arrow::Status status = arrow::py::ConvertTableToPandas(options, table_to_convert, &pandas_df);
    if (!status.ok()) {
        throw std::runtime_error("Failed to convert Arrow Table to Pandas DataFrame: " + status.ToString());
    }

    // Wrap the resulting PyObject* as a py::object
    return py::reinterpret_steal<py::object>(pandas_df);
}



py::object load_parquet(py::dict pyEnv) {
    spdlog::info("Initializing XClient and starting buffer fetch...");

    // Initialize XClient from pyEnv
    xdbc::RuntimeEnv env;
    env.env_name = pyEnv["env_name"].cast<std::string>();
    env.table = pyEnv["table"].cast<std::string>();
    env.iformat = pyEnv["iformat"].cast<int>();
    env.buffer_size = pyEnv["buffer_size"].cast<int>();
    env.buffers_in_bufferpool = pyEnv["bufferpool_size"].cast<int>() / pyEnv["buffer_size"].cast<int>();
    env.sleep_time = std::chrono::milliseconds(pyEnv["sleep_time"].cast<int>());
    env.rcv_parallelism = pyEnv["rcv_parallelism"].cast<int>();
    env.write_parallelism = pyEnv["write_parallelism"].cast<int>();
    env.ser_parallelism = pyEnv["write_parallelism"].cast<int>();
    env.decomp_parallelism = pyEnv["decomp_parallelism"].cast<int>();
    env.transfer_id = pyEnv["transfer_id"].cast<long>();
    env.server_host = pyEnv["server_host"].cast<std::string>();
    env.server_port = pyEnv["server_port"].cast<std::string>();
    env.skip_serializer = pyEnv["skip_ser"].cast<int>();

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

    // Start the XClient
    xdbc::XClient client(env);
    spdlog::info("XClient initialized for table: {} and schema {}", env.table, env.schemaJSON);

    client.startReceiving(env.table);
    spdlog::info("Started receiving buffers from XClient.");

    std::vector<std::shared_ptr<arrow::Table>> tables;

    // Fetch and process buffers from XClient
    while (client.hasNext(0)) {
        xdbc::buffWithId curBuffWithId = client.getBuffer(0);
        if (curBuffWithId.id < 0) {
            spdlog::warn("Invalid buffer ID: {}", curBuffWithId.id);
            continue;
        }

        // Wrap the buffer in an Arrow Buffer
        auto arrow_buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(curBuffWithId.buff), curBuffWithId.totalSize
        );

        // Create an Arrow BufferReader
        auto buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);

        // Open Parquet file reader
        std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
        auto status = parquet::arrow::OpenFile(buffer_reader, arrow::default_memory_pool(), &parquet_reader);
        if (!status.ok()) {
            spdlog::error("Failed to open Parquet file: {}", status.ToString());
            continue;
        }

        // Read the entire Parquet file into an Arrow Table
        std::shared_ptr<arrow::Table> current_table;
        status = parquet_reader->ReadTable(&current_table);
        if (!status.ok()) {
            spdlog::error("Failed to read Parquet table: {}", status.ToString());
            continue;
        }

        //spdlog::info("Successfully read Parquet table from buffer ID: {}", curBuffWithId.id);

        tables.push_back(current_table);

        // Mark the buffer as read
        client.markBufferAsRead(curBuffWithId.id);
    }
    client.finalize();

    spdlog::info("All buffers processed. Combining Arrow tables...");

    // Concatenate Arrow tables if there are multiple
    std::shared_ptr<arrow::Table> final_table;
    if (tables.size() == 1) {
        final_table = tables[0];
    } else {
        auto concat_result = arrow::ConcatenateTables(tables);
        if (!concat_result.ok()) {
            throw std::runtime_error("Failed to concatenate Arrow tables: " + concat_result.status().ToString());
        }
        final_table = concat_result.ValueOrDie();
    }

    // Combine chunks within the final Arrow Table
    auto combine_result = final_table->CombineChunks(arrow::default_memory_pool());
    if (!combine_result.ok()) {
        throw std::runtime_error("Failed to combine Arrow chunks: " + combine_result.status().ToString());
    }

    final_table = combine_result.ValueOrDie();

    spdlog::info("Deserialization completed. Converting to Pandas DataFrame...");

    //return convertArrowTableToPandas(final_table);
    return py::reinterpret_steal<py::object>(
            arrow::py::wrap_table(final_table)
        );
}

PYBIND11_MODULE(pyxdbcparquet, m) {
    m.doc() = "XDBC parquet loader";
    m.def("load", &load_parquet, "Load Parquet file buffers via XClient and convert to Pandas DataFrame");
}

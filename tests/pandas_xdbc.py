import pyxdbc
import numpy as np
import pandas as pd
import datetime
import argparse
from inspect import getmembers, isfunction
import schemata

parser = argparse.ArgumentParser(description="Script to configure the PyXDBC Client environment.")

parser.add_argument('--env_name', type=str, required=True, help="The name of the environment.")
parser.add_argument('--table', type=str, required=True, help="The name of the table.")
parser.add_argument('--iformat', type=int, required=True, help="The input format.")
parser.add_argument('--buffer_size', type=int, required=True, help="The buffer size in bytes.")
parser.add_argument('--bufferpool_size', type=int, required=True, help="The buffer pool size in bytes.")
parser.add_argument('--sleep_time', type=int, required=True, help="The sleep time in seconds.")
parser.add_argument('--rcv_par', type=int, required=True, help="The receive parallelism level.")
parser.add_argument('--write_par', type=int, required=True, help="The write parallelism level.")
parser.add_argument('--decomp_par', type=int, required=True, help="The decompression parallelism level.")
parser.add_argument('--transfer_id', type=int, required=True, help="The transfer ID.")
parser.add_argument('--server_host', type=str, required=True, help="The server host name.")
parser.add_argument('--server_port', type=str, required=True, help="The server port.")

args = parser.parse_args()
# build cmd: `cmake .. -DCMAKE_BUILD_TYPE=Release && make && cp pyxdbc.cpython-39-x86_64-linux-gnu.so ../tests/ && python ../tests/pandas_xdbc.py `
np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)  # or 1000
pd.set_option('display.max_rows', None)  # or 1000
pd.set_option('display.max_colwidth', None)  # or 199

a = datetime.datetime.now()
print(getmembers(pyxdbc), isfunction)

schema = schemata.schema[args.table]

total_tuples = schemata.sizes[args.table]
pyenv = {
    'table': args.table,
    'env_name': args.env_name,
    'iformat': args.iformat,
    'buffer_size': args.buffer_size,
    'bufferpool_size': args.bufferpool_size,
    'sleep_time': args.sleep_time,
    'rcv_parallelism': args.rcv_par,
    'write_parallelism': args.write_par,
    'decomp_parallelism': args.decomp_par,
    'transfer_id': args.transfer_id,
    'server_host': "xdbcserver",
    'server_port': "1234",
    'total_tuples': total_tuples
}

data = pyxdbc.load(pyenv)

'''
local_arr = np.array([1, 2, 3])
print("local array")
print(local_arr.shape)
print(local_arr)
'''
print("remote array")
print(data[0].shape)

# print(list[0])

start_df = datetime.datetime.now()
column_names = [col[0] for col in schema]
data_dict = {column_names[i]: data[i] for i in range(len(column_names))}
dataset = pd.DataFrame(data_dict, copy=False)
end_df = datetime.datetime.now()
print("df creation:")
print((end_df - start_df).total_seconds() * 1000)

b = datetime.datetime.now()
c = b - a
print("total time:")
print(c.total_seconds() * 1000)

# with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
# print(dataset.count())
# print(dataset.head(10))
print("min: " + str(dataset[schemata.keys[args.table]].min()))
print("max: " + str(dataset[schemata.keys[args.table]].max()))
print("mean: " + str(dataset[schemata.keys[args.table]].mean()))
print("count: " + str(dataset[schemata.keys[args.table]].count()))

print(dataset.head(10))

# print( np.isclose(np.sum(np.sin(a)), xdbc.sum_of_sines(a)))

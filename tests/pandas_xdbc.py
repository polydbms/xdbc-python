import pyxdbc
import numpy as np
import pandas as pd
import datetime
from inspect import getmembers, isfunction

#build cmd: `cmake .. -DCMAKE_BUILD_TYPE=Release && make && cp pyxdbc.cpython-39-x86_64-linux-gnu.so ../tests/ && python ../tests/pandas_xdbc.py `
np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)  # or 1000
pd.set_option('display.max_rows', None)  # or 1000
pd.set_option('display.max_colwidth', None)  # or 199

a = datetime.datetime.now()
print(getmembers(pyxdbc), isfunction)

schema = [
    ('l_orderkey', 'I'),
    ('l_partkey', 'I'),
    ('l_suppkey', 'I'),
    ('l_linenumber', 'I'),
    ('l_quantity', 'D'),
    ('l_extendedprice', 'D'),
    ('l_discount', 'D'),
    ('l_tax', 'D'),
    ('l_returnflag', 'C'),
    ('l_linestatus', 'C'),
    ('l_shipdate', 'S'),
    ('l_commitdate', 'S'),
    ('l_receiptdate', 'S'),
    ('l_shipinstruct', 'S'),
    ('l_shipmode', 'S'),
    ('l_comment', 'S')
]

total_tuples = 59986052
table_name = "lineitem_sf10"
pyenv = {
    'env_name': "PyXDBC Client",
    'table': table_name,
    'iformat': 1,
    'buffer_size': 1024,
    'bufferpool_size': 65536,
    'sleep_time': 1,
    'rcv_parallelism': 1,
    'write_parallelism': 8,
    'decomp_parallelism': 1,
    'transfer_id': 0,
    'server_host': "xdbcserver",
    'server_port': "1234",
    'profiling_breakpoint': 100,
}

data = pyxdbc.load(table_name, total_tuples, pyenv)

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
print((end_df-start_df).total_seconds() * 1000)

b = datetime.datetime.now()
c = b - a
print("total time:")
print(c.total_seconds() * 1000)

# with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
# print(dataset.count())
# print(dataset.head(10))
print("min: " + str(dataset['l_orderkey'].min()))
print("max: " + str(dataset['l_orderkey'].max()))
print("mean: " + str(dataset['l_orderkey'].mean()))
print("count: " + str(dataset['l_orderkey'].count()))


print(dataset.head(10))

# print( np.isclose(np.sum(np.sin(a)), xdbc.sum_of_sines(a)))

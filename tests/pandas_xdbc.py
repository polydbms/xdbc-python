import pyxdbc
import pyxdbcparquet
import pyarrow as pa
import numpy as np
import pandas as pd
import datetime
import argparse
from inspect import getmembers, isfunction
import schemata

# python3 pandas_xdbc.py --env_name="PyXDBC" --table="lineitem_sf10" --iformat=2 --buffer_size=1024 --bufferpool_size=65536 --sleep_time=1 --rcv_par 1 --write_par=1 --decomp_par=1 --transfer_id=0 --server_host=xdbcserver --server_port=1234 --source=csv --skip_ser=0
parser = argparse.ArgumentParser(description="Script to configure the PyXDBC Client environment.")

parser.add_argument('--env_name', type=str, required=True, help="The name of the environment.")
parser.add_argument('--table', type=str, required=True, help="The name of the table.")
parser.add_argument('--iformat', type=int, required=True, help="The input format.")
parser.add_argument('--buffer_size', type=int, required=True, help="The buffer size in bytes.")
parser.add_argument('--bufferpool_size', type=int, required=True, help="The buffer pool size in bytes.")
parser.add_argument('--sleep_time', type=int, required=True, help="The sleep time in seconds.")
parser.add_argument('--rcv_par', type=int, required=True, help="The receive parallelism level.")
parser.add_argument('--write_par', type=int, required=True, help="The write parallelism level.")
parser.add_argument('--ser_par', type=int, required=True, help="The serialization parallelism level.")
parser.add_argument('--decomp_par', type=int, required=True, help="The decompression parallelism level.")
parser.add_argument('--transfer_id', type=int, required=True, help="The transfer ID.")
parser.add_argument('--server_host', type=str, required=True, help="The server host name.")
parser.add_argument('--server_port', type=str, required=True, help="The server port.")
parser.add_argument('--source', type=str, required=True, help="The source system.")
parser.add_argument('--skip_ser', type=int, required=True, help="Skip de-serialization.")
parser.add_argument('--debug', type=int, default=0, help="Print statistics on received dataset.")

args = parser.parse_args()
# build cmd: `cd /workspace/build && cmake .. -DCMAKE_BUILD_TYPE=Debug && make && cp pyxdbc.cpython-39-x86_64-linux-gnu.so ../tests/`
np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)  # or 1000
pd.set_option('display.max_rows', None)  # or 1000
pd.set_option('display.max_colwidth', None)  # or 199

a = datetime.datetime.now()
#print(getmembers(pyxdbcparquet), isfunction)

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
    'total_tuples': total_tuples,
    'skip_ser': args.skip_ser
}

import pandas as pd
import numpy as np

def reconstruct_dataframe(blocks, column_names):
    """
    Reconstruct a Pandas DataFrame from Arrow blocks.
    :param blocks: List of dictionaries with 'block' and 'placement' keys.
    :return: Pandas DataFrame.
    """
    if not isinstance(blocks, list):
        raise ValueError("Input must be a list of blocks")

    # Initialize an empty dictionary to hold columns
    columns = {}

    for block in blocks:
        # Validate the block structure
        if not isinstance(block, dict):
            raise ValueError("Each block must be a dictionary")
        if 'block' not in block or 'placement' not in block:
            raise ValueError("Each block must contain 'block' and 'placement' keys")

        block_data = block['block']
        placement = block['placement']

        # Ensure placement is a 1D array of integers
        if not isinstance(placement, np.ndarray) or placement.ndim != 1:
            raise ValueError("'placement' must be a 1D NumPy array")

        # Ensure block_data is a 2D array
        if not isinstance(block_data, np.ndarray) or block_data.ndim != 2:
            raise ValueError("'block' must be a 2D NumPy array")

        # Map block columns to their placement indices
        for idx, col_idx in enumerate(placement):
            columns[col_idx] = block_data[idx]

    # Sort columns by their indices and combine them
    sorted_columns = [columns[key] for key in sorted(columns.keys())]
    data = np.column_stack(sorted_columns)

    # Create a Pandas DataFrame
    return pd.DataFrame(data, columns = column_names)


column_names = [col[0] for col in schema]

if args.source=="parquet" and args.skip_ser==1:
    arrow_table = pyxdbcparquet.load(pyenv)
    print("finished XDBC part")
    #dataset = reconstruct_dataframe(data, column_names)
    dataset = arrow_table.to_pandas(split_blocks=True, self_destruct=True)
    #print(f"Dataset size: {dataset.shape[0]} rows, {dataset.shape[1]} columns")
else:
    print("Entering normal XDBC Client")
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
    att_orders = ('S', 'I', 'D', 'C')

    start_df = datetime.datetime.now()

    # data_dict = {column_names[i]: data[i] for i in range(len(column_names))}
    dataset = schemata.reassign_columns(schema, data, att_orders)
    end_df = datetime.datetime.now()
    print("df creation:")
    print((end_df - start_df).total_seconds() * 1000)


b = datetime.datetime.now()
c = b - a
print("total time:")
print(c.total_seconds() * 1000)

if args.debug==1:
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    # print(dataset.count())
    # print(dataset.head(10))
    print("min: " + str(dataset[schemata.keys[args.table]].min()))
    print("max: " + str(dataset[schemata.keys[args.table]].max()))
    print("mean: " + str(dataset[schemata.keys[args.table]].mean()))
    print("count: " + str(dataset[schemata.keys[args.table]].count()))

    print(dataset.head(10))


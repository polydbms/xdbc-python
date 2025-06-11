import numpy as np
import pandas as pd
import datetime
import schemata
import argparse
import fsspec
import duckdb

parser = argparse.ArgumentParser(description="Script to configure the Pandas baselines.")
parser.add_argument('--table', type=str, required=True, help="The remote directory containing Parquet files.")
parser.add_argument('--debug', type=int, default=0, help="Print statistics on the dataset.")
parser.add_argument('--system', type=str, default="pyarrow", help="pyarrow/duckdb")

args = parser.parse_args()

np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

start_time = datetime.datetime.now()

def create_dataframe_from_remote_parquet(remote_directory):
    combined_df = pd.DataFrame()

    # Use fsspec to list all Parquet files in the remote directory
    fs = fsspec.filesystem("http")
    file_infos = fs.ls(remote_directory)

    # Extract full URLs for Parquet files
    parquet_files = [file_info['name'] for file_info in file_infos if file_info['name'].endswith('.parquet')]

    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in remote directory: {remote_directory}")

    if args.system == "pyarrow":
        print("Using pyarrow loader")
        # Read all Parquet files and concatenate them into a single DataFrame
        dataframes = [pd.read_parquet(file, storage_options={"protocol": "http"}, engine='pyarrow') for file in parquet_files]
        combined_df = pd.concat(dataframes, ignore_index=True)
    else:
        print("Using duckdb loader")

        query = " UNION ALL ".join(
            [f"SELECT * FROM read_parquet('{file}')" for file in parquet_files]
        )
        #query = f"SELECT * FROM read_parquet(query)"
        #query = "SELECT * FROM read_parquet('http://xdbcserver:1234/lineitem_big.parquet')"
        combined_df = duckdb.query(query).to_df()

    return combined_df




# Example usage
remote_parquet_dir = f"http://xdbcserver:1234/{args.table}"
column_names = [col[0] for col in schemata.schema[args.table]]

dataset = create_dataframe_from_remote_parquet(remote_parquet_dir)

end_time = datetime.datetime.now()
duration = end_time - start_time
print(f"Time taken: {duration.total_seconds() * 1000:.2f} ms")

if args.debug == 1:
    print(f"min: {dataset[schemata.keys[args.table]].min()}")
    print(f"max: {dataset[schemata.keys[args.table]].max()}")
    print(f"mean: {dataset[schemata.keys[args.table]].mean()}")
    print(f"count: {dataset[schemata.keys[args.table]].count()}")
    print(dataset.head(10))

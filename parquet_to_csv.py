import pyarrow.parquet as pq
import pyarrow.csv as csv
import duckdb
import argparse
import os
import glob
import time
import fsspec


def is_http_path(path):
    """Check if a path is an HTTP or HTTPS URL."""
    return path.startswith("http://") or path.startswith("https://")


def list_remote_files(remote_dir):
    """List all Parquet files in a remote HTTP directory."""
    fs = fsspec.filesystem("http")
    file_infos = fs.ls(remote_dir)

    # Extract full URLs for Parquet files
    parquet_files = [file_info['name'] for file_info in file_infos if file_info['name'].endswith('.parquet')]

    return parquet_files


def convert_with_pyarrow(parquet_file, csv_file):
    """Convert Parquet to CSV using PyArrow."""
    table = pq.read_table(parquet_file)
    csv.write_csv(table, csv_file)
    print(f"Converted {parquet_file} to {csv_file} using PyArrow.")


def convert_with_duckdb(parquet_file, csv_file):
    """Convert Parquet to CSV using DuckDB."""
    conn = duckdb.connect(database=":memory:")
    parquet_path = f"'{parquet_file}'" if not is_http_path(parquet_file) else f"'{parquet_file}' (protocol=http)"
    conn.execute(f"COPY (SELECT * FROM read_parquet({parquet_path})) TO '{csv_file}' WITH (FORMAT CSV, HEADER TRUE);")
    print(f"Converted {parquet_file} to {csv_file} using DuckDB.")


def convert_directory_pyarrow(input_dir, output_dir, remote=False):
    """Convert all Parquet files in a directory to CSV using PyArrow."""
    parquet_files = list_remote_files(input_dir) if remote else glob.glob(os.path.join(input_dir, "*.parquet"))
    for parquet_file in parquet_files:
        csv_file = os.path.join(output_dir, os.path.basename(parquet_file).replace(".parquet", ".csv"))
        convert_with_pyarrow(parquet_file, csv_file)


def convert_directory_duckdb(input_dir, output_dir, remote=False):
    """Convert all Parquet files in a directory to CSV using DuckDB."""
    print("Remote or Local DuckDB")
    conn = duckdb.connect(database=":memory:")
    parquet_files = list_remote_files(input_dir) if remote else glob.glob(os.path.join(input_dir, "*.parquet"))
    for parquet_file in parquet_files:
        csv_file = os.path.join(output_dir, os.path.basename(parquet_file).replace(".parquet", ".csv"))
        parquet_path = f"'{parquet_file}'"
        q = f"COPY (SELECT * FROM read_parquet({parquet_path})) TO '{csv_file}' WITH (FORMAT CSV, HEADER TRUE);"
        #print(q)
        conn.execute(q)
        print(f"Converted {parquet_file} to {csv_file} using DuckDB.")


def main():
    parser = argparse.ArgumentParser(description="Convert Parquet to CSV using PyArrow or DuckDB.")
    parser.add_argument('--system', type=str, choices=['pyarrow', 'duckdb'], required=True, help="System to use for conversion: 'pyarrow' or 'duckdb'.")
    parser.add_argument('--filename', type=str, required=True, help="Path to a Parquet file or directory containing Parquet files.")
    parser.add_argument('--output', type=str, required=True, help="Output file or directory for CSV files.")

    args = parser.parse_args()

    input_path = args.filename
    output_path = args.output
    system = args.system
    remote = is_http_path(input_path)

    print(f"Is remote: {remote}")

    start_time = time.time_ns()  # Start timing

    if remote or os.path.isdir(input_path):
        # Handle a directory of Parquet files (local or remote)
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        if system == 'pyarrow':
            convert_directory_pyarrow(input_path, output_path, remote)
        elif system == 'duckdb':
            convert_directory_duckdb(input_path, output_path, remote)
    elif os.path.isfile(input_path) or remote:
        # Handle a single Parquet file
        if os.path.isdir(output_path):
            # If the output path is a directory, create a CSV file with the same name
            output_file = os.path.join(output_path, os.path.basename(input_path).replace(".parquet", ".csv"))
        else:
            # If the output path is a file, use it directly
            output_file = output_path

        if system == 'pyarrow':
            convert_with_pyarrow(input_path, output_file)
        elif system == 'duckdb':
            convert_with_duckdb(input_path, output_file)
    else:
        print("Invalid input path. Please provide a valid file or directory.")

    end_time = time.time_ns()  # End timing
    elapsed_time = (end_time - start_time) / 1e9  # Convert to seconds
    print(f"Total execution time: {elapsed_time:.6f} seconds.")


if __name__ == "__main__":
    main()

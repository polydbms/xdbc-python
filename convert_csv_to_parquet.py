import duckdb
import json
import argparse
import os
import math

#  python3 convert_csv_to_parquet.py --csv_file /dev/shm/ss13husallm.csv --schema_file /xdbc-client/tests/schemas/ss13husallm.json --output_dir /dev/shm --max_size_mb 64
# Parse arguments
parser = argparse.ArgumentParser(description="Split CSV into Parquet files of fixed size using DuckDB.")
parser.add_argument('--csv_file', type=str, required=True, help="Path to the input CSV file.")
parser.add_argument('--schema_file', type=str, required=True, help="Path to the JSON schema file.")
parser.add_argument('--output_dir', type=str, required=True, help="Directory to store the Parquet splits.")
parser.add_argument('--max_size_mb', type=int, required=True, help="Maximum size of each Parquet file in MB.")
args = parser.parse_args()

# Load schema
with open(args.schema_file, 'r') as f:
    schema = json.load(f)

# Create output directory named after the CSV file (without .csv)
base_name = os.path.splitext(os.path.basename(args.csv_file))[0]
output_dir = os.path.join(args.output_dir, base_name)
os.makedirs(output_dir, exist_ok=True)

# Prepare column definitions for DuckDB
columns = []
column_definitions = []
for idx, col in enumerate(schema):
    col_name = col['name']
    if col['type'] == 'INT':
        columns.append(col_name)
        column_definitions.append(f"{col_name} INTEGER")
    elif col['type'] == 'DOUBLE':
        columns.append(col_name)
        column_definitions.append(f"{col_name} DOUBLE")
    elif col['type'] == 'CHAR':
        columns.append(col_name)
        column_definitions.append(f"{col_name} CHAR({col['size']})")
    elif col['type'] == 'STRING':
        columns.append(col_name)
        column_definitions.append(f"{col_name} VARCHAR({col['size']})")
column_definitions_sql = ", ".join(column_definitions)

delim = ","
if base_name == "lineitem_sf10":
    delim = "|"
# Load CSV into DuckDB in-memory table with schema
conn = duckdb.connect()
conn.execute(f"""
    CREATE TABLE temp_table ({column_definitions_sql});
""")
conn.execute(f"""
    COPY temp_table FROM '{args.csv_file}' (DELIMITER '{delim}', HEADER FALSE);
""")

# Calculate rows per split
row_count = conn.execute("SELECT COUNT(*) FROM temp_table").fetchone()[0]
bytes_per_row = sum(col['size'] for col in schema)
rows_per_split = math.floor((args.max_size_mb * 1024 * 1024) / bytes_per_row)

# Split and write Parquet files
part_number = 0
for start in range(0, row_count, rows_per_split):
    end = min(start + rows_per_split, row_count)
    parquet_file = os.path.join(output_dir, f"{base_name}_part{part_number}.parquet")
    query = f"""
        COPY (
            SELECT * FROM temp_table
            LIMIT {rows_per_split} OFFSET {start}
        ) TO '{parquet_file}' (FORMAT PARQUET);
    """
    conn.execute(query)
    print(f"Written: {parquet_file}")
    part_number += 1

print("Splitting completed.")

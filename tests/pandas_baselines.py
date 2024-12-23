import pandas as pd
import psycopg2
import datetime
from sqlalchemy import create_engine
import connectorx as cx
from turbodbc import connect, make_options, Rows, Megabytes
import psycopg2
import duckdb
import modin.pandas as mpd
import ray
import argparse
import schemata

parser = argparse.ArgumentParser(description="Script to configure the Pandas baselines.")

parser.add_argument('--parallelism', type=int, required=True, help="The amount of threads to use.")
parser.add_argument('--chunksize', type=int, required=True, help="The size of the chunk.")
parser.add_argument('--library', type=str, required=True, help="The library to use for copying the dataframe.")
parser.add_argument('--table', type=str, required=True, help="The table to copy.")
args = parser.parse_args()


# run cmd: `python3 /workspace/tests/pandas_baselines.py --library "connectorx" --table "lineitem_sf10" --parallelism 8 --chunksize 100`

def give_df_psycopg2(table):
    conn = psycopg2.connect("dbname=db1 user=postgres password=123456 host='pg1' port=5432")
    sql = "select * from " + str(table)
    dat = pd.read_sql_query(sql, conn)
    conn = None
    return dat


def give_df_sqlalchemy(table):
    alchemyEngine = create_engine('postgresql+psycopg2://postgres:123456@pg1:5432/db1', pool_recycle=3600)

    dbConnection = alchemyEngine.connect()

    return pd.read_sql("SELECT * FROM " + str(table), dbConnection)


def give_df_connectorx(table):
    return cx.read_sql("postgresql://postgres:123456@pg1:5432/db1", "SELECT * FROM " + str(table),
                       protocol="binary",
                       # protocol="csv",
                       partition_on=schemata.keys[args.table], partition_num=args.parallelism)


def give_df_turbodbc(table, method=""):
    options = make_options(
        use_async_io=True,
        read_buffer_size=Megabytes(args.chunksize)
    )

    connection = connect(turbodbc_options=options,
                         connection_string='Driver={PostgreSQL ANSI};Server=pg1;Port=5432;Database=db1;Uid=postgres;Pwd=123456;')
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM " + str(table))

    if method == 'arrow':
        table = cursor.fetchallarrow()
        return table.to_pandas()
    else:
        return pd.DataFrame(cursor.fetchallnumpy())


def give_duckdb(table_name):
    conn_str = "dbname=db1 user=postgres password=123456 host=pg1 port=5432"
    con = duckdb.connect()
    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres;")
    con.execute(f"CALL postgres_attach('{conn_str}');")

    query = f"SELECT * FROM postgres_scan('{conn_str}', 'public', '{table_name}');"

    df = con.execute(query).fetchdf()
    con.close()

    return df


def give_modin(table_name):
    if args.parallelism:
        ray.init(num_cpus=args.parallelism)

    conn_str = "postgresql://postgres:123456@pg1:5432/db1"

    engine = create_engine(conn_str)

    query = f"SELECT * FROM {table_name}"
    modin_df = mpd.read_sql(query, con=engine)

    return modin_df


a = datetime.datetime.now()
table = args.table

print(f"Starting data transfer for {args.library}, table {table}")
if args.library == 'turbodbc-arrow':
    dataset = give_df_turbodbc(table, method="arrow")
elif args.library == 'turbodbc':
    dataset = give_df_turbodbc(table)
elif args.library == 'connectorx':
    dataset = give_df_connectorx(table)
elif args.library == 'sqlalchemy':
    dataset = give_df_sqlalchemy(table)
elif args.library == 'psycopg2':
    dataset = give_df_psycopg2(table)
elif args.library == 'duckdb':
    dataset = give_duckdb(table)
elif args.library == 'modin':
    dataset = give_modin(table)
else:
    print("No valid library")

print("min: " + str(dataset[schemata.keys[args.table]].min()))
print("max: " + str(dataset[schemata.keys[args.table]].max()))
print("mean: " + str(dataset[schemata.keys[args.table]].mean()))
print("count: " + str(dataset[schemata.keys[args.table]].count()))
b = datetime.datetime.now()
c = b - a
print(c.total_seconds() * 1000)

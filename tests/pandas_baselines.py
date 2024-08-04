import pandas as pd
import psycopg2
import datetime
from sqlalchemy import create_engine
import connectorx as cx
from turbodbc import connect
import psycopg2


def give_df_psycopg2(table):
    conn = psycopg2.connect("dbname=db1 user=postgres password=123456 host='pg1' port=5432")
    sql = "select * from " + str(table)
    dat = pd.read_sql_query(sql, conn)
    conn = None
    return dat


def give_df_sqlalchemy(table):
    alchemyEngine = create_engine('postgresql+psycopg2://postgres:123456@pg1:5432/db1', pool_recycle=3600);

    dbConnection = alchemyEngine.connect();

    return pd.read_sql("SELECT * FROM " + str(table), dbConnection)


def give_df_connectorx(table):
    return cx.read_sql("postgresql://postgres:123456@pg1:5432/db1", "SELECT * FROM " + str(table),
                       protocol="binary",
                       partition_on="l_orderkey", partition_num=4)


def give_df_turbodbc(table, method=""):
    connection = connect(
        connection_string='Driver={PostgreSQL ANSI};Server=pg1;Port=5432;Database=db1;Uid=postgres;Pwd=123456;')
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM " + str(table))

    if method == 'arrow':
        table = cursor.fetchallarrow()
        return table.to_pandas()
    else:
        return pd.DataFrame(cursor.fetchallnumpy())


a = datetime.datetime.now()
table = "lineitem_sf10"
#dataset = give_df_turbodbc(table, method="arrow")
#dataset = give_df_turbodbc(table)
#dataset = give_df_connectorx(table)
#dataset = give_df_sqlalchemy(table)
dataset = give_df_psycopg2(table)

print("min: " + str(dataset['l_orderkey'].min()))
print("max: " + str(dataset['l_orderkey'].max()))
print("mean: " + str(dataset['l_orderkey'].mean()))
print("count: " + str(dataset['l_orderkey'].count()))
b = datetime.datetime.now()
c = b - a
print(c.total_seconds() * 1000)

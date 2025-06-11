import numpy as np
import pandas as pd
import datetime
import schemata
import argparse

parser = argparse.ArgumentParser(description="Script to configure the Pandas baselines.")
parser.add_argument('--table', type=str, required=True, help="The table to copy.")
parser.add_argument('--debug', type=int, default=0, help="Print statistics on received dataset.")

args = parser.parse_args()

np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)  # or 1000
pd.set_option('display.max_rows', None)  # or 1000
pd.set_option('display.max_colwidth', None)  # or 199

a = datetime.datetime.now()

# Function to read a CSV file and create a DataFrame with a given schema
def create_dataframe_from_csv(filename, column_names, delim=','):
    # Read the CSV file into a pandas DataFrame with specified column names
    df = pd.read_csv(filename, names=column_names, sep=delim, header=None, engine="pyarrow")
    return df


# Example usage
delim = ','
ext = '.csv'
if args.table == 'lineitem_sf10':
    delim = '|'
    ext = '.tbl'

url = 'http://xdbcserver:1234/' + args.table + ext

column_names = [col[0] for col in schemata.schema[args.table]]

dataset = create_dataframe_from_csv(url, column_names, delim)

b = datetime.datetime.now()
c = b - a
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


import os
import tensorflow_io as tfio
import tensorflow as tf
import numpy as np

endpoint="postgresql://{}:{}@{}?port={}&dbname={}".format(
    'postgres',
    '123456',
    'localhost',
    '15432',
    'db1',
)


dataset = tfio.experimental.IODataset.from_sql(
    query="SELECT l_orderkey, l_partkey FROM test;",
    endpoint=endpoint)

print(dataset.element_spec)


# check only the first 20 record
dataset = dataset.take(200)

print("NOx - NO2:")
for lineitem in dataset:
  print(lineitem['l_orderkey'].numpy())

'''
for i in dataset:
    print(i['l_orderkey'].numpy())
'''
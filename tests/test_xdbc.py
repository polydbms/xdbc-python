import pyxdbc
import numpy as np
import pandas as pd
import datetime
from inspect import getmembers, isfunction
np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)  # or 1000
pd.set_option('display.max_rows', None)  # or 1000
pd.set_option('display.max_colwidth', None)  # or 199

a = datetime.datetime.now()
print(getmembers(pyxdbc), isfunction)
c = pyxdbc.XClient("Client")
print("Made an xdbc client called: %s" % c.get_name())
list = c.load("lineitem.l_orderkey")

'''
local_arr = np.array([1, 2, 3])
print("local array")
print(local_arr.shape)
print(local_arr)
'''
print("remote array")
print(list[0].shape)
# print(list[0])

dataset = pd.DataFrame({'l_orderkey': list[0][:, 0],
                        'l_partkey': list[0][:, 1],
                        'l_suppkey': list[0][:, 2],
                        'l_linenumber': list[0][:, 3],
                        'l_quantity': list[1][:, 0],
                        'l_extendedprice': list[1][:, 1],
                        'l_discount': list[1][:, 2],
                        'l_tax': list[1][:, 3]})
# with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
# print(dataset.count())
print("min: " + str(dataset['l_orderkey'].min()))
print("max: " + str(dataset['l_orderkey'].max()))
print("mean: " + str(dataset['l_orderkey'].mean()))
print("count: " + str(dataset['l_orderkey'].count()))
b = datetime.datetime.now()
c = b - a
print(c.total_seconds()*1000)

# print( np.isclose(np.sum(np.sin(a)), xdbc.sum_of_sines(a)))

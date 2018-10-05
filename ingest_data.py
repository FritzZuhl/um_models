import os

import numpy as np
import pandas as py

os.getcwd()

d = py.read_csv("d3_am.csv", sep=';')


d.ndim
d.shape

np.savetxt(r'd3_am.txt', d)
d = np.loadtxt(r'd3_am_from_pandas.csv')

save(r'd3_am.npy', d)


d_gp = d.groupby('listener_id')


def count_grp(df):
    """

    :type df: object
    """
    return df.shape

count = d_gp(count_grp)

d_gp.


for x in d_gp:
    print d_gp.shape

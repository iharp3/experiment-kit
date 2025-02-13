import os
import numpy as np
import tiledb

# need code to find indexes for lat/long/time slice you need
# *** need code to aggregate by spatial resolution
# *   need code to aggregate over time

"""
Get array index corresponding to given time.
ARGS:   a (tiledb.Array)
        t (np.datetime)
OUT:    i (int)
"""
def get_time_idx(a, t):
    
    pass
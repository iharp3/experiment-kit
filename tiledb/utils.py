import os
import json
import numpy as np
import pandas as pd
import tiledb

json_file = "/data/experiment-kit/tiledb/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f)

def get_time_indices(start_time, end_time):
        starting_idx = int((start_time - inputs["start_time"]).total_seconds()/3600)
        ending_idx = int((end_time - inputs["start_time"]).total_seconds()/3600)    # [starting_idx, ending_idx) 

        return starting_idx, ending_idx

def get_spatial_range(max_lat, min_lat, max_lon, min_lon):
        max_lat_idx = max_lat + inputs["latitude_shift"]
        min_lat_idx = min_lat + inputs["latitude_shift"]
        max_lon_idx = max_lon + inputs["longitude_shift"]
        min_lon_idx = min_lon + inputs["longitude_shift"]

        return max_lat_idx, min_lat_idx, max_lon_idx, min_lon_idx

def time_aggregation(res, data):

        """
        res: string ("day", "month", "year")
        data: list of tiledb dense arrays
        """

        # define a function that aggregates each array by a specific time (day, month, year) by taking in the necessary indices, and using aggregation with slicing OR just pandas/numpy (like GPT example)

        # if statements for all the different time resolutions

        # .apply or use UDF to send list of arrays to aggregate and return the aggregate...which can be 1) a new array that you delete after you use or 2) a numpy/panda object that you use vanilla code (?) on to get the answer you want?

        # MULTI-ARRAY DF EXAMPLE: https://documentation.cloud.tiledb.com/academy/analyze/user-defined-functions/#multi-array-udfs


def spatial_aggregation(res, data): # *same as time aggregation function*
        pass
        # you can just aggregate that section by aggregating every 4 values for every resolution drop


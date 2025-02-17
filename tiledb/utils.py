import os
import json
import numpy as np
import pandas as pd
import tiledb

json_file = "/data/experiment-kit/tiledb/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f)

def get_time_indices(start_time, end_time):
        starting_idx = int((pd.to_datetime(start_time) - pd.to_datetime(inputs["start_time"])).total_seconds()/3600)
        ending_idx = int((pd.to_datetime(end_time) - pd.to_datetime(inputs["start_time"])).total_seconds()/3600)    # [starting_idx, ending_idx) 

        return starting_idx, ending_idx

def get_spatial_range(max_lat, min_lat, max_lon, min_lon):
        max_lat_idx = max_lat + inputs["latitude_shift"]
        min_lat_idx = min_lat + inputs["latitude_shift"]
        max_lon_idx = max_lon + inputs["longitude_shift"]
        min_lon_idx = min_lon + inputs["longitude_shift"]

        return int(max_lat_idx), int(min_lat_idx), int(max_lon_idx), int(min_lon_idx)

def get_index_pairs(timestamps, time_res, start_time):
    time_shift = int((pd.to_datetime(start_time) - pd.to_datetime(inputs["start_time"])).total_seconds()/3600) # moves relative index pairs to correct part of array indices
    timestamps = timestamps.to_timestamp()
    index_pairs = []
    start_idx = 0
    for i in range(1, len(timestamps)):
        if time_res == "day":
            if timestamps[i].date() != timestamps[i - 1].date():  # checks day is the same
                index_pairs.append((start_idx+time_shift, i+time_shift))
                start_idx = i  # Update start index for new day
        elif time_res == "month":
            if pd.to_datetime(timestamps[i]).month != pd.to_datetime(timestamps[i - 1]).month:    # checks month is the same
                index_pairs.append((start_idx+time_shift, i+time_shift))
                start_idx = i  # Update start index for new month
        elif time_res == "year":
            if pd.to_datetime(timestamps[i]).year != pd.to_datetime(timestamps[i - 1]).year:    # checks year is the same
                index_pairs.append((start_idx+time_shift, i+time_shift))
                start_idx = i  # Update start index for new year
        else:
            return ValueError(f"Invalid temporal resolution {time_res}")

    if start_idx <= len(timestamps)-1:
         pass
    else:
        index_pairs.append((start_idx+time_shift, (len(timestamps) -1)+time_shift)) # last pair -> end of timestamps

    # print(index_pairs)
    return index_pairs

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


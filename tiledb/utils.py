import os
import json
import numpy as np
import pandas as pd
import tiledb
from proposed.utils.get_whole_period import get_whole_period_between

json_file = "/data/experiment-kit/tiledb/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f)

def get_day_idx(day: int) -> int:
       idx = (24 * day) - 24
       return idx

def get_time_indices(start_time, end_time):
        y, m, d, h = get_whole_period_between(start=pd.Timestamp(start_time), end=pd.Timestamp(end_time))
        if y:
                print("YEARS:", [i for i in range(y[0], y[-1] + 1, 1)])
        if m:
                print("MONTHS:")
                print(f"year: {pd.Timestamp(m[0]).year}")
                idx = [i for i in range(int(pd.Timestamp(m[0]).month), int(pd.Timestamp(m[-1]).month) + 1, 1)]
                for i in idx:
                        print(pd.Timestamp(year=pd.Timestamp(m[0]).year,month=i,day=1).month_name())
        if d:
                print("DAYS:")
                print(f"year: {pd.Timestamp(d[0]).year}, month: {pd.Timestamp(d[0]).month_name()}")
                idx = [i for i in range(int(pd.Timestamp(d[0]).day), int(pd.Timestamp(d[-1]).day) + 1, 1)]
                print(idx)
        if h:
                print(f"year: {pd.Timestamp(h[0]).year}, month: {pd.Timestamp(h[0]).month_name()}, day: {pd.Timestamp(h[-1]).day}")
                idx = [i for i in range(int(pd.Timestamp(h[0]).hour), int(pd.Timestamp(h[-1]).hour) + 1, 1)]
                print(idx)
        # for each whole year, you have to open the correct array and deal with it, 
        # the indices are all of them
        # for the month residuals, you get the year and slice just those months, 
        # so need the start index of the first month and the end index of the last month
        # for the day residuals, you need to know the starting index (which should be at the last month) 
        # get all the month/day/hour indices for leap years and standard years (the hour months start and end, the hour days start)

        # define a function that, given the hour a month starts, can give you the starting hour index of a specified day, and the ending hour index of a specified day

        # define a function that, given the starting hour of a day, can give you the starting hour index of a specified hour


def get_spatial_range(max_lat, min_lat, max_lon, min_lon):
        pass
        # define a function that gives you the start and end indices for a specified spatial region (only need to think about 0.25 res)

def get_raster(*args):
        pass
        # use get_time_range and get_spatial range to return a raster (?) [CHECK what proposed returns: .nc?] 

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


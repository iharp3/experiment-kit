import os
import sys
import json
import numpy as np
import pandas as pd
import xarray as xr
import tiledb

from get_whole_period import get_whole_period_between


from tiledb_get_raster_executor import tiledb_get_raster_executor
from tiledb_get_heatmap_executor import tiledb_get_heatmap_executor
from tiledb_get_timeseries_executor import tiledb_get_timeseries_executor
from tiledb_find_area_executor import tiledb_find_area_executor
from tiledb_find_time_executor import tiledb_find_time_executor

json_file = "/data/experiment-kit/tiledb/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f)

def load_dataset(nc_path, file_name):
    ds = xr.open_dataset(os.path.join(nc_path, file_name))
    return ds

def check_dims_raw_files():
    same = True
    start = True
    prev_file = ""
    for file in os.listdir(inputs["nc_data_dir"]):
        # if same:
        if file.endswith(".nc"):
            if start:
                ds_prev = load_dataset(inputs["nc_data_dir"], file)
                prev_file = file
                print(f"\n\n")
                print(f"File: {file}")
                print(ds_prev.dims)
                # print(f"\n")
                start = False
                continue
            ds_cur = load_dataset(inputs["nc_data_dir"], file)
            print(f"\n")
            print(f"File: {file}")
            print(ds_cur.dims)
            # print(f"\n")

            # compare latitude values
            if (ds_prev["latitude"].values == ds_cur["latitude"].values).all():
                pass
            else:
                print(f"\n\t\tValues are not the same\n\n{len((ds_prev["latitude"].values != ds_cur["latitude"].values).nonzero()[0])}")
                # same = False

            # compare longitude values
            if (ds_prev["longitude"].values == ds_cur["longitude"].values).all():
                pass
            else:
                print(f"\n\t\tValues are not the same\n\n{len((ds_prev["longitude"].values != ds_cur["longitude"].values).nonzero()[0])}")
                # same = False

            # compare month/day/hour/minute values
            d1_time = pd.to_datetime(ds_prev["time"].values)
            d2_time = pd.to_datetime(ds_cur["time"].values)
            d1_reduced = [(t.month, t.day, t.hour, t.minute) for t in d1_time]
            d2_reduced = [(t.month, t.day, t.hour, t.minute) for t in d2_time]
            if np.array_equal(d1_reduced, d2_reduced):
                print("The month-day-hour-minute values match exactly.")
            else:
                print(f"\t\tMismatches found between {file} and {prev_file}.")
                # same = False

            prev_file = file

        # else:
        #     sys.exit()

def explore_whole_period_between(s,e):
    y, m, d, h = get_whole_period_between(s, e)
    got_year = got_month = got_day = got_hour = False
    print(f"\nYEAR: {y}")
    print(f"\nMONTH: {m}")
    print(f"\nDAY: {d}")
    print(f"\nHOUR: {h}")

    if y:
        got_year = True
        print("YEARS:")
        print([i for i in range(y[0], y[-1] + 1,1)])    # list of all complete years
        y1 = int(inputs["year_indices_hours"][str(y[0])])   # get the index of the first hour of the first year
        y2 = int(inputs["year_indices_hours"][str(y[-1] + 1)]) - 1  # get the last hour of the last year
        print(y1, y2) 
    if m:
        got_month = True
        print("MONTHS:")
        year = pd.Timestamp(m[0]).year
        if not got_year:
            y1 = y2 = int(year)
        print(f"yr: {year}") # incomplete year
        idx = [pd.Timestamp(year=pd.Timestamp(m[0]).year,month=i,day=1).month_name() for i in range(int(pd.Timestamp(m[0]).month), int(pd.Timestamp(m[-1]).month) + 1, 1)] # name of the months you want
        print(idx)
        if year in inputs["leap_years"]:
            m1 = inputs["leap_year_month_indices"][idx[0]][0]  # index of first hour of the month
            m2 = inputs["leap_year_month_indices"][idx[-1]][1] #index of last hour of the month
        else:
            m1 = inputs["standard_year_month_indices"][idx[0]][0]  # index of first hour of month
            m2 = inputs["standard_year_month_indices"][idx[-1]][1] # index of last hour of month
        print(m1, m2)
    if d:
        got_day = True
        print("DAYS:")
        year = pd.Timestamp(d[0]).year
        month = pd.Timestamp(d[0]).month_name()
        if not got_year:
            y1 = y2 = int(year)
        if not got_month:
            m1 = m2 = int(month)
        print(f"yr: {year}, mo: {month}")
        # print(f"\n\td:{d}")
        idx = [i for i in range(int(pd.Timestamp(d[0]).day), int(len(d) +1 ), 1)]  # list of all days
        print(idx)
        d1 = get_hour_idx(idx[0])    # index of hour of first day
        d2 = get_last_hour_idx(idx[-1])   # index of hour of last day
        print(d1, d2)
    if h:
        got_hour = True
        print("HOURS:")
        year = pd.Timestamp(h[0]).year
        month = pd.Timestamp(h[0]).month_name()
        day = pd.Timestamp(h[-1]).day
        if not got_year:
            y1 = y2 = int(year)
        if not got_month:
            m1 = m2 = int(month)
        if not got_day:
            d1 = d2 = int(day)
        print(f"yr: {year}, mo: {month}, day: {day}")
        idx = [i for i in range(int(pd.Timestamp(h[0]).hour), int(pd.Timestamp(h[-1]).hour) + 1, 1)]
        print(idx)
        h1 = idx[0]     # first hour
        h2 = idx[-1]    # last hour 
        print(h1, h2)
    
    if not got_hour:
        h1 = 0
        h2 = 0

    print(f"y1: {y1}, m1: {m1}, d1: {d1}, h1: {h1}")
    print(f"y2: {y2}, m2: {m2}, d2: {d2}, h2: {h2}")
    start_idx = y1 + m1 + d1 + h1
    end_idx = y2 + m2 + d2 + h2
    print(f"starting index: {start_idx}")
    print(f"ending index: {end_idx}")

def open_tiledb_array():
    # Open one of the arrays (replace with your variable name, e.g., temperature)
    with tiledb.open(inputs["tiledb_data_dir"], mode="r") as array:
        # Example: Query all data for a specific time slice
        temp_data = array[:, :, 0]  # First time slice
        print("\nTemperature data for first time slice:", temp_data)
        print(f"\nType: {type(array)}")
        print(f"\nShape: {array.shape}")
        print(f"\nSchema: {array.schema}")

    array.close()

def compare_array_aggs():
    ds = load_dataset(inputs["nc_data_dir"])
    with tiledb.open(inputs["tiledb_data_dir"], mode="r") as array:
        pass
    array.close()

def xr_temp_agg(ds):
    daily_avg_temp = ds["temperature"].resample(time="1D").mean()
    return daily_avg_temp

def tdb_temp_agg(ar):
    pass

def get_last_hour_idx(day: int) -> int:
    return (24*day)

def get_hour_idx(day: int) -> int:
    idx = (24 * day) - 24
    return idx

def get_index_pairs(timestamps, time_res, start_time):
    time_shift = int((pd.to_datetime(start_time) - pd.to_datetime(inputs["start_time"])).total_seconds()/3600)  # moves relative index pairs to correct part of array indices
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

    index_pairs.append((start_idx+time_shift, (len(timestamps) -1)+time_shift)) # last pair -> end of timestamps

    # print(index_pairs)
    return index_pairs

if __name__ == "__main__":
    """Testing util functions"""
    
    # open_tiledb_array()

    # s = pd.Timestamp("2015-06-01 00:00")
    # e = pd.Timestamp("2016-02-28 00:00")
    # temporal_resolution = "year"
    # timestamps = pd.period_range(start=s, end=e, freq="h")
    # index_pairs = get_index_pairs(timestamps=timestamps, time_res=temporal_resolution, start_time=s)  # TODO: self.temporal_resolution
    # print(index_pairs)

    """Testing get_raster"""

    # executor = tiledb_get_raster_executor(
    #     variable="temperature",  # Change to an available variable in the dataset
    #     start_datetime="2014-01-01 00:00",
    #     end_datetime="2014-06-01 00:00",
    #     temporal_resolution="day",
    #     min_lat=0.0,
    #     max_lat=40.0,
    #     min_lon=0.0,
    #     max_lon=40.0,
    #     spatial_resolution=0.5,
    #     aggregation="mean"
    #     )
    

    """Testing get_heatmap"""       # TODO: test 1.0, test min, max
    # executor = tiledb_get_heatmap_executor(
    #     variable="temperature",  # Change to an available variable in the dataset
    #     start_datetime="2014-01-01 00:00",
    #     end_datetime="2014-06-01 00:00",
    #     temporal_resolution="day",
    #     min_lat=30.0,
    #     max_lat=40.0,
    #     min_lon=-100.0,
    #     max_lon=-90.0,
    #     spatial_resolution=0.5,
    #     aggregation="mean"      
    #     )
    
    """Testing get_timeseries"""
    # executor = tiledb_get_timeseries_executor(
    #     variable="temperature",  # Change to an available variable in the dataset
    #     start_datetime="2014-01-01 00:00",
    #     end_datetime="2014-06-01 00:00",
    #     temporal_resolution="day",
    #     min_lat=30.0,
    #     max_lat=40.0,
    #     min_lon=-100.0,
    #     max_lon=-90.0,
    #     spatial_resolution=0.5,
    #     aggregation="mean"
    #     )
    
    
    """Testing find_area"""
    executor = tiledb_find_area_executor(
        variable="temperature",  # Change to an available variable in the dataset
        start_datetime="2014-01-01 00:00",
        end_datetime="2014-06-01 00:00",
        temporal_resolution="day",
        min_lat=30.0,
        max_lat=40.0,
        min_lon=-100.0,
        max_lon=-90.0,
        spatial_resolution=0.5,
        aggregation="mean",
        filter_predicate= "<",
        filter_value=360.0
        )
    result = executor.execute()
    print("\nfind area\n", result[:5,:5])
    
    """Testing find_time"""
    executor = tiledb_find_time_executor(
        variable="temperature",  # Change to an available variable in the dataset
        start_datetime="2014-01-01 00:00",
        end_datetime="2014-06-01 00:00",
        temporal_resolution="day",
        min_lat=30.0,
        max_lat=40.0,
        min_lon=-100.0,
        max_lon=-90.0,
        spatial_resolution=0.5,
        aggregation="mean",
        filter_predicate= "<",
        filter_value=360.0
        )

    result = executor.execute()
    print("\nfind time\n",result[:5])
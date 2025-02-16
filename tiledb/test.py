import os
import sys
import json
import numpy as np
import pandas as pd
import xarray as xr
import tiledb

from get_whole_period import get_whole_period_between, get_whole_ranges_between

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

if __name__ == "__main__":
    # open_tiledb_array()
    s = pd.Timestamp("2005-01-01 00:00")
    e = pd.Timestamp("2005-03-12 08:00")

    y, m, d, h = get_whole_period_between(s, e)
    if y:
        print("YEARS:", [i for i in range(y[0], y[-1] + 1,1)])
    if m:
        print("MONTHS:")
        print(f"year: {pd.Timestamp(m[0]).year}") #, start month: {pd.Timestamp(m[0]).month_name()}, end month: {pd.Timestamp(m[-1]).month_name()}")
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
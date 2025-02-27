import os
import json
import numpy as np
import pandas as pd
import tiledb
import xarray as xr

json_file = "/data/experiment-kit/tiledb2/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f)


def get_time_indices(start_time, end_time):
    starting_idx = int((pd.to_datetime(start_time) - pd.to_datetime(inputs["start_time"])).total_seconds() / 3600)
    ending_idx = int(
        (pd.to_datetime(end_time) - pd.to_datetime(inputs["start_time"])).total_seconds() / 3600
    )  # [starting_idx, ending_idx)

    return starting_idx, ending_idx


def get_spatial_range(max_lat, min_lat, max_lon, min_lon):
    max_lat_idx = get_025_idx(max_lat, inputs["latitude_shift"])
    min_lat_idx = get_025_idx(min_lat, inputs["latitude_shift"])
    max_lon_idx = get_025_idx(max_lon, inputs["longitude_shift"])
    min_lon_idx = get_025_idx(min_lon, inputs["longitude_shift"])

    return int(max_lat_idx), int(min_lat_idx), int(max_lon_idx), int(min_lon_idx)


def get_025_idx(x, shift):
    return 4 * (shift + x)


def get_agg_function(agg):
    if agg == "mean":
        agg_function = np.mean
    elif agg == "max":
        agg_function = np.max
    elif agg == "min":
        agg_function = np.min
    else:
        return ValueError(f"Invalid aggregation {agg}")
    return agg_function


def get_coord_block(res):
    if res == 0.25:
        b = 1
    elif res == 0.5:
        b = 2
    elif res == 1:
        b = 4
    else:
        return ValueError(f"Invalid spatial resolution {res}")
    return b


def get_index_pairs(timestamps, time_res, start_time):
    time_shift = int(
        (pd.to_datetime(start_time) - pd.to_datetime(inputs["start_time"])).total_seconds() / 3600
    )  # moves relative index pairs to correct part of array indices
    timestamps = timestamps.to_timestamp()
    index_pairs = []
    start_idx = 0
    for i in range(1, len(timestamps)):
        if time_res == "day":
            if timestamps[i].date() != timestamps[i - 1].date():  # checks day is the same
                index_pairs.append((start_idx + time_shift, i + time_shift))
                start_idx = i  # Update start index for new day
        elif time_res == "month":
            if (
                pd.to_datetime(timestamps[i]).month != pd.to_datetime(timestamps[i - 1]).month
            ):  # checks month is the same
                index_pairs.append((start_idx + time_shift, i + time_shift))
                start_idx = i  # Update start index for new month
        elif time_res == "year":
            if pd.to_datetime(timestamps[i]).year != pd.to_datetime(timestamps[i - 1]).year:  # checks year is the same
                index_pairs.append((start_idx + time_shift, i + time_shift))
                start_idx = i  # Update start index for new year
        else:
            return ValueError(f"Invalid temporal resolution {time_res}")

    if start_idx <= len(timestamps) - 1:
        pass
    else:
        index_pairs.append(
            (start_idx + time_shift, (len(timestamps) - 1) + time_shift)
        )  # last pair -> end of timestamps

    # print(index_pairs)
    return index_pairs


def nparray_to_xarray(nparray, start_time, end_time, min_lat, max_lat, min_lon, max_lon):
    ds_empty = xr.Dataset()
    ds_empty["latitude"] = np.arange(-90, 90.1, 0.25)
    ds_empty["longitude"] = np.arange(-180, 180.1, 0.25)
    ds_empty["time"] = pd.date_range(
        start=start_time,
        end=end_time,
        freq="h",
    )
    ds_empty = ds_empty.sel(
        latitude=slice(min_lat, max_lat),
        longitude=slice(min_lon, max_lon),
    )
    # fine trim ds_empty to match the shape of nparray
    len_time, len_lat, len_lon = nparray.shape
    ds_empty = ds_empty.isel(
        time=slice(0, len_time),
        latitude=slice(0, len_lat),
        longitude=slice(0, len_lon),
    )
    ds_empty["temperature"] = xr.DataArray(
        data=nparray,
        dims=["time", "latitude", "longitude"],
        coords={
            "time": ds_empty["time"],
            "latitude": ds_empty["latitude"],
            "longitude": ds_empty["longitude"],
        },
    )
    return ds_empty


def temporal_resolution_to_freq(resolution):
    if resolution == "hour":
        return "h"
    elif resolution == "day":
        return "D"
    elif resolution == "month":
        return "ME"
    elif resolution == "year":
        return "YE"
    else:
        raise ValueError("Invalid temporal_resolution")

import os
import sys
import json
import numpy as np
import pandas as pd
import xarray as xr
import tiledb

json_file = "/data/experiment-kit/tiledb/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f)

def load_dataset(nc_path, file_name):
    ds = xr.open_dataset(os.path.join(nc_path, file_name))
    return ds

if __name__ == "__main__":
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



# # Open one of the arrays (replace with your variable name, e.g., temperature)
# with tiledb.open(tiledb_data_dir, mode="r") as array:
#     # Example: Query all data for a specific time slice
#     temp_data = array[:, :, 0]  # First time slice
#     print("\nTemperature data for first time slice:", temp_data)
#     print(f"\nType: {type(array)}")
#     print(f"\nShape: {array.shape}")
#     print(f"\nSchema: {array.schema}")

# array.close()
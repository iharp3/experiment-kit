import os
import sys
import json
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
    for file in os.listdir(inputs["nc_data_dir"]):
        if same:
            if file.endswith(".nc"):
                if start:
                    ds_prev = load_dataset(inputs["nc_data_dir"], file)
                    print(f"\n\n")
                    print(f"File: {file}")
                    print(f"\n\n")
                    print(ds_prev.dims)
                    print(f"\n\n")
                    # print(ds.coords)
                    start = False
                    continue
                ds_cur = load_dataset(inputs["nc_data_dir"], file)
                print(f"\n\n")
                print(f"File: {file}")
                print(f"\n\n")
                print(ds_prev.dims)
                print(f"\n\n")
                # print(ds.coords)

                # compare datasets:
                if ds_prev["time"] == ds_cur["time"]:
                    pass
                else:
                    print(f"\n\n\t\t{ds_prev["time"]}")
                    print(f"\n\n\t\t{ds_cur["time"]}")
                    same = False

        else:
            sys.exit()



# # Open one of the arrays (replace with your variable name, e.g., temperature)
# with tiledb.open(tiledb_data_dir, mode="r") as array:
#     # Example: Query all data for a specific time slice
#     temp_data = array[:, :, 0]  # First time slice
#     print("\nTemperature data for first time slice:", temp_data)
#     print(f"\nType: {type(array)}")
#     print(f"\nShape: {array.shape}")
#     print(f"\nSchema: {array.schema}")

# array.close()
import os
import xarray as xr
import tiledb

nc_data_dir = "/data/iharp-customized-storage/storage/514_data"
tiledb_data_dir = "/data/iharp-customized-storage/storage/experiments_tdb"
eg_file = "2m_temperature-2014.nc"


def load_dataset(nc_path, file_name):
    ds = xr.open_dataset(os.path.join(nc_path, file_name))
    return ds

if __name__ == "__main__":
    ds = load_dataset(nc_data_dir, eg_file)
    print(f"\n\n")
    print(ds.values)
    print(f"\n\n")
    print(ds.dims)
    print(f"\n\n")
    print(ds.coords)
    print(f"\n\n")
    print(ds.attrs)

# # Open one of the arrays (replace with your variable name, e.g., temperature)
# with tiledb.open(tiledb_data_dir, mode="r") as array:
#     # Example: Query all data for a specific time slice
#     temp_data = array[:, :, 0]  # First time slice
#     print("\nTemperature data for first time slice:", temp_data)
#     print(f"\nType: {type(array)}")
#     print(f"\nShape: {array.shape}")
#     print(f"\nSchema: {array.schema}")

# array.close()
import os
import numpy as np
import xarray as xr
import tiledb
# import tiledb.cf  # Did not work - dependency issues
# from tiledb.cf import AttrMetadata, ArrayMetadata     # Did not work - dependency issues

# mkdir -p /data/tiledb_arrays
# export TILEDB_DEFAULT_STORAGE_PATH="/data/tiledb_arrays"

nc_data_dir = "/data/iharp-customized-storage/storage/514_data"
tiledb_data_dir = "/data/iharp-customized-storage/storage/experiments_tdb"

ds = xr.open_dataset(os.path.join(nc_data_dir, "2m_temperature-2014.nc"))
chunked = ds.chunk({"time": 8760, "latitude": 10, "longitude": 10})
print(chunked.chunksizes)
chunked['t2m'].data.to_tiledb(os.path.join(tiledb_data_dir))

# # Define time range   
# start_date = np.datetime64("2014-01-01")
# end_date = np.datetime64("2015-01-01")

# # Convert to integers (in days) for domain definition
# start_time = (start_date - np.datetime64("1970-01-01")) // np.timedelta64(1, 'D')
# end_time = (end_date - np.datetime64("1970-01-01")) // np.timedelta64(1, 'D')


# # Define dimensions
# lat_dim = tiledb.Dim(name="latitude", domain=(-90.0, 90.0), tile=10, dtype=np.float64)
# lon_dim = tiledb.Dim(name="longitude", domain=(-180.0, 180.0), tile=10, dtype=np.float64)
# time_dim = tiledb.Dim(name="time", domain=(start_time, end_time), tile=365, dtype=np.int64)  # Days since a reference point

# # Define the domain
# domain = tiledb.Domain(lat_dim, lon_dim, time_dim)

# # Define attributes (e.g., temperature)
# temperature = tiledb.Attr(name="t2m", dtype=np.float32, filters=[tiledb.ZstdFilter(level=5)])

# # Create the schema
# schema = tiledb.ArraySchema(domain=domain, attrs=[temperature])

# # Create the array
# tiledb.DenseArray.create("climate_data_array", schema)

# # Load a NetCDF file
# ds = xr.open_dataset(os.path.join(nc_data_dir, "2m_temperature-2014.nc"))

# # Example: Write temperature data to TileDB
# with tiledb.DenseArray("climate_data_array", mode="w") as array:
#     array[:] = ds["t2m"].values

# with tiledb.DenseArray("climate_data_array", mode="r") as array:
#     # Query a specific time slice
#     temperature_slice = array[:, :, 0]  # Query the first time slice
#     print(temperature_slice)
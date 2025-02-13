import os
import json
import numpy as np
import xarray as xr
import tiledb
import shutil

json_file = "/data/experiment-kit/tiledb/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f)

""" 
Run the following in terminal:

# mkdir -p <directory for tiledb arrays>
# export TILEDB_DEFAULT_STORAGE_PATH="<directory for tiledb arrays>"
"""

# ------------          took 15 minutes for 100/62208 chunks to load
def get_time_range(ds):
    s_date = ds.time.min().values
    e_date = ds.time.max().values

    if isinstance(s_date, np.datetime64):
        pass
    else:
        print(f"\n\tStarting date type: {type(s_date)}")
        s_date = np.datetime64(s_date)

    if isinstance(e_date, np.datetime64):
        pass
    else:
        print(f"\n\tStarting date type: {type(e_date)}")
        e_date = np.datetime64(e_date)

    return s_date, e_date

def delete_schema(path):
    print(f"\nDeleting schema")
    if tiledb.object_type(path) == "array":
        shutil.rmtree(path)
        print(f"\tDeleted TileDB array at {path}")
    else:
        print("\tNo TileDB array found at the specified path.")

def load_dataset(nc_path, file_name):
    print(f"\nLoading dataset {file_name}")
    ds = xr.open_dataset(os.path.join(nc_path, file_name))
    return ds

def chunk_dataset(ds, t, lat, lon):
    chunked = ds.chunk({"time": t, "latitude": lat, "longitude": lon})
    return chunked

def write_chunked_data_from_one(data, array_path, chunk_sizes):
    with tiledb.DenseArray(array_path, mode="w") as array:
        time_chunks, lat_chunks, lon_chunks = chunk_sizes

        total_chunks = (data.shape[0] // time_chunks) * (data.shape[1] // lat_chunks) * (data.shape[2] // lon_chunks)
        print(f"\n\tTotal chunks to process: {total_chunks}")

        counter = 0
        for t_start in range(0, data.shape[0], time_chunks):
            for lat_start in range(0, data.shape[1], lat_chunks):
                for lon_start in range(0, data.shape[2], lon_chunks):
                    t_end = min(t_start + time_chunks, data.shape[0])
                    lat_end = min(lat_start + lat_chunks, data.shape[1])
                    lon_end = min(lon_start + lon_chunks, data.shape[2])

                    # Write chunk to the array
                    array[t_start:t_end, lat_start:lat_end, lon_start:lon_end] = \
                        data[t_start:t_end, lat_start:lat_end, lon_start:lon_end]
                    
                    counter += 1
                    if counter%50==0:
                        print(f"\t\tChunk {counter}/{total_chunks} written")

def write_chunked_data(data, i_t, time_chunks=inputs["t_chunk"], lat_chunks=inputs["lat_chunk"], lon_chunks=inputs["lon_chunk"]):

    total_chunks = (data.shape[0] // time_chunks) * (data.shape[1] // lat_chunks) * (data.shape[2] // lon_chunks)
    print(f"\n\tTotal chunks to process: {total_chunks}")

    counter = 0
    for t_start in range(0, data.shape[0], time_chunks):
        for lat_start in range(0, data.shape[1], lat_chunks):
            for lon_start in range(0, data.shape[2], lon_chunks):
                t_end = min(t_start + time_chunks, data.shape[0])
                lat_end = min(lat_start + lat_chunks, data.shape[1])
                lon_end = min(lon_start + lon_chunks, data.shape[2])

                # Write chunk to the array
                array[i_t+t_start:i_t+t_end, lat_start:lat_end, lon_start:lon_end] = \
                    data[t_start:t_end, lat_start:lat_end, lon_start:lon_end]
                
                counter += 1
                if counter%100==0:
                    print(f"\t\tChunk {counter}/{total_chunks} written")

def make_array(t, lat, lon):
    print(f"\nMaking array")
    # All Dim in dense tiledb Array must be the same *integer* type
    time_dim = tiledb.Dim(name="time", domain=(0, inputs["time_idx_max"]), tile=t, dtype=np.uint64)
    lat_dim = tiledb.Dim(name="latitude", domain=(0, inputs["lat_idx_max"]), tile=lat, dtype=np.uint64)
    lon_dim = tiledb.Dim(name="longitude", domain=(0, inputs["lon_idx_max"]), tile=lon, dtype=np.uint64)
    # print(f"\n\tCreated dimensions")
    
    domain = tiledb.Domain(time_dim, lat_dim, lon_dim)
    # print(f"\n\tCreated domain")
    
    attr = tiledb.Attr(name="temperature", dtype=np.float64)
    # print(f"\n\tCreated attribute")

    schema = tiledb.ArraySchema(
        domain=domain,
        attrs=[attr],
        cell_order="row-major",
        tile_order="row-major",
        sparse=False,
    )
    # print(f"\n\tCreated schema")

    tiledb.DenseArray.create(inputs["tiledb_data_dir"], schema)
    # print(f"\n\tCreated densearray")
    return 

def make_array_from_one(ds, t, lat, lon):
    print(f"\nMaking array")
    # All Dim in dense tiledb Array must be the same *integer* type
    time_dim = tiledb.Dim(name="time", domain=(0, ds.sizes["time"] - 1), tile=t, dtype=np.uint64)
    lat_dim = tiledb.Dim(name="latitude", domain=(0, ds.sizes["latitude"] - 1), tile=lat, dtype=np.uint64)
    lon_dim = tiledb.Dim(name="longitude", domain=(0, ds.sizes["longitude"] - 1), tile=lon, dtype=np.uint64)
    # print(f"\n\tCreated dimensions")
    
    domain = tiledb.Domain(time_dim, lat_dim, lon_dim)
    # print(f"\n\tCreated domain")
    
    attr = tiledb.Attr(name="temperature", dtype=np.float64)
    # print(f"\n\tCreated attribute")

    schema = tiledb.ArraySchema(
        domain=domain,
        attrs=[attr],
        cell_order="row-major",
        tile_order="row-major",
        sparse=False,
    )
    # print(f"\n\tCreated schema")

    tiledb.DenseArray.create(inputs["tiledb_data_dir"], schema)
    # print(f"\n\tCreated densearray")

    write_chunked_data_from_one(ds["t2m"].data, inputs["tiledb_data_dir"], (t, lat, lon))

if __name__ == "__main__":
    
    # delete_schema(inputs["tiledb_data_dir"])

    if inputs["all_files"]=="True":     # for all .nc files in folder
        make_array(t=inputs["t_chunk"], lat=inputs["lat_chunk"], lon=inputs["lon_chunk"])
        t_idx = 0

        for file in os.listdir(inputs["nc_data_dir"]):
            if file.endswith(".nc"):
                ds = load_dataset(inputs["nc_data_dir"], file)
                c = chunk_dataset(ds, t=inputs["t_chunk"], lat=inputs["lat_chunk"], lon=inputs["lon_chunk"])
                
                with tiledb.DenseArray(inputs["tiledb_data_dir"], mode="w") as array:
                    t_size, lat_size, lon_size = write_chunked_data(c[inputs["var"]].data, t_idx, inputs["t_chunk"], inputs["lat_chunk"], inputs["lon_chunk"])
                    t_idx += t_size
                    # lat_idx += lat_size   #TODO: figure out how to make sure the data has same dim values at each interval
                    # lon_idx += lon_size

    else:   # for one .nc file
        ds = load_dataset(inputs["nc_data_dir"], inputs["eg_file"])
        c = chunk_dataset(ds, t=inputs["t_chunk"], lat=inputs["lat_chunk"], lon=inputs["lon_chunk"])
        make_array_from_one(c, t=inputs["t_chunk"], lat=inputs["lat_chunk"], lon=inputs["lon_chunk"])

""" --------------------------------------- Graveyard --------------------------------------- """
# ------------ PROBLEM: RuntimeWarning Engine 'tiledb' loading failed: module 'tiledb.libtiledb' has no attribute 'Metadata' ------------------ #
# ------------          slow (took ~45 mins for 1 yr), can't edit dim names, etc., trying to find better way ------------------ 

# if tiledb.object_type(tiledb_data_dir) == "array":
#     shutil.rmtree(tiledb_data_dir)
#     print(f"Deleted TileDB array at {tiledb_data_dir}")
# else:
#     print("No TileDB array found at the specified path.")

# ds = xr.open_dataset(os.path.join(nc_data_dir, "2m_temperature-2014.nc"))
# chunked = ds.chunk({"time": 24, "latitude": 10, "longitude": 10})
# # print(chunked.chunksizes)
# chunked['t2m'].data.to_tiledb(os.path.join(tiledb_data_dir))

# ------------ PROBLEM: module 'tiledb.libtiledb' has no attribute 'Metadata ------------------ #
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

# ------------ PROBLEM: module 'tiledb.libtiledb' has no attribute 'Metadata ------------------ #
# config = tiledb.Config({"vfs.file.enable_filelocks": "false"})  # Recommended for local storage
# ctx = tiledb.Ctx(config)
 
# converter = tiledb.cf.NetCDFToTileDBConverter(output_group_path=tiledb_data_dir)
 
# file_list = ["2m_temperature-2014.nc"]
# # for filename in os.listdir(nc_data_dir):
# for filename in file_list:
#     if filename.endswith(".nc"):
#         file_path = os.path.join(nc_data_dir, filename)
#         print(f"Processing file: {file_path}")
#         converter.add_input_file(input_file=file_path)
 
#  # Perform the conversion
# converter.convert()
# print("Conversion complete. Data stored in TileDB at:", tiledb_data_dir)
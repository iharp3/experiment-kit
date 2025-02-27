import os
import json
import numpy as np
import xarray as xr
import tiledb
import shutil

json_file = "/data/experiment-kit/tile/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f)

""" 
Run the following in terminal:

# mkdir -p <directory for tiledb arrays>
# export TILEDB_DEFAULT_STORAGE_PATH="<directory for tiledb arrays>"
"""

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
    final_time_idx = 0
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
                if counter%(total_chunks//5)==0:
                    print(f"\t\tChunk {counter}/{total_chunks} written")
                final_time_idx = t_end
    print(f"\n\tfinal time idx: {final_time_idx}")
    return final_time_idx

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
    
    delete_schema(inputs["tiledb_data_dir"])

    if inputs["all_files"]=="True":     # for all .nc files in folder
        make_array(t=inputs["t_chunk"], lat=inputs["lat_chunk"], lon=inputs["lon_chunk"])
        t_idx = 0

        prev_file = ""
        for file in os.listdir(inputs["nc_data_dir"]):
            if file.endswith(".nc"):
                print(f"\n\tLoading file {file}")
                ds = load_dataset(inputs["nc_data_dir"], file)
                c = chunk_dataset(ds, t=inputs["t_chunk"], lat=inputs["lat_chunk"], lon=inputs["lon_chunk"])
                
                with tiledb.DenseArray(inputs["tiledb_data_dir"], mode="w") as array:
                    t_size = write_chunked_data(c[inputs["var"]].data, t_idx, inputs["t_chunk"], inputs["lat_chunk"], inputs["lon_chunk"])
                    t_idx += t_size
                
                # deleting previous file
                if os.path.exists(os.path.join(inputs["nc_data_dir"], prev_file)):
                    os.remove(prev_file)
                    print(f"{file} has been deleted.")
                prev_file = file

        tiledb.consolidate(inputs["tiledb_data_dir"])
        tiledb.vacuum(inputs["tiledb_data_dir"])

    else:   # for one .nc file
        ds = load_dataset(inputs["nc_data_dir"], inputs["eg_file"])
        c = chunk_dataset(ds, t=inputs["t_chunk"], lat=inputs["lat_chunk"], lon=inputs["lon_chunk"])
        make_array_from_one(c, t=inputs["t_chunk"], lat=inputs["lat_chunk"], lon=inputs["lon_chunk"])
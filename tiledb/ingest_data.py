import os
import tiledb
import tiledb.cf

# mkdir -p /data/tiledb_arrays
# export TILEDB_DEFAULT_STORAGE_PATH="/data/tiledb_arrays"

nc_data_dir = "/data/iharp-customized-storage/storage/514_data"
tiledb_data_dir = "/data/iharp-customized-storage/storage/experiments_tdb"

config = tiledb.Config({"vfs.file.enable_filelocks": "false"})  # Recommended for local storage
ctx = tiledb.Ctx(config)

converter = tiledb.cf.NetCDFToTileDBConverter(output_group_path=tiledb_data_dir)

file_list = ["2m_temperature-2014.nc"]
# for filename in os.listdir(nc_data_dir):
for filename in file_list:
    if filename.endswith(".nc"):
        file_path = os.path.join(nc_data_dir, filename)
        print(f"Processing file: {file_path}")
        converter.add_input_file(input_file=file_path)

# Perform the conversion
converter.convert()
print("Conversion complete. Data stored in TileDB at:", tiledb_data_dir)
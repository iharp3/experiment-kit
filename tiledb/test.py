import tiledb

tiledb_data_dir = "/data/iharp-customized-storage/storage/experiments_tdb"


# Open one of the arrays (replace with your variable name, e.g., temperature)
with tiledb.open(tiledb_data_dir, mode="r") as array:
    # Example: Query all data for a specific time slice
    temp_data = array[:, :, 0]  # First time slice
    print("Temperature data for first time slice:", temp_data)
    # print(array.schema)
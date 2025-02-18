import multiprocessing
import pandas as pd
import sys
import time

# add the path to the sys.path
sys.path.append("..")
from tiledb.tiledb_get_raster_executor import tiledb_get_raster_executor

def run_query(q):
    start_time = time.time()
    qe = tiledb_get_raster_executor(
        variable=q["variable"],
        start_datetime=q["start_time"],
        end_datetime=q["end_time"],
        max_lat=q["max_lat"],
        min_lat=q["min_lat"],
        min_lon=q["min_lon"],
        max_lon=q["max_lon"],
        spatial_resolution=q["spatial_resolution"],
        temporal_resolution=q["temporal_resolution"],
        aggregation=q["aggregation"],
    )
    try:
        qe.execute()
    except Exception as e:
        print(q)
        print(e)
        return -1
    return time.time() - start_time


def main():
    df_query = pd.read_csv("queries/get_raster_test_set_tiledb.csv")

    num_cores = max(1, multiprocessing.cpu_count() - 3)
    print(f"Using {num_cores} cores")

    with multiprocessing.Pool(process=num_cores) as pool:
        time_list = pool.map(run_query, df_query.to_dict(orient="records"))
        
    df_query["execution_time"] = time_list
    current_time = time.strftime("%m%d-%H%M%S")
    df_query.to_csv(f"results/tiledb_get_raster_test_result_{current_time}.csv", index=False)

if __name__ == "__main__":
    main()
import pandas as pd
import sys
import time
import os

# add the path to the sys.path
main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(main_dir)
sys.path.append(main_dir)
from vanilla.vanilla_get_raster_executor import VanillaGetRasterExecutor

# sys.path.append("../vanilla")
# from vanilla_get_raster_executor import VanillaGetRasterExecutor


def run_query(q):
    start_time = time.time()
    qe = VanillaGetRasterExecutor(
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
        print(e)
        return -1
    return time.time() - start_time


if __name__ == "__main__":
    df_query = pd.read_csv("GR_test_0227.csv")

    time_list = []
    for query in df_query.to_records():
        print(query)
        execution_time = run_query(query)
        print(execution_time)
        time_list.append(execution_time)
        print("======================\n")

    df_query["execution_time"] = time_list
    current_time = time.strftime("%m%d-%H%M%S")
    df_query.to_csv(f"vanilla_GR_result_{current_time}.csv", index=False)

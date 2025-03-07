import pandas as pd
import sys
import time

# add the path to the sys.path
import os
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
        print(q)
        print(e)
        return -1
    return time.time() - start_time


if __name__ == "__main__":
    df_query = pd.read_csv("/home/uribe055/experiment-kit/experiment/queries/get_raster_test_set_025Hchanging_temp_res.csv")

    time_list = []

    for query in df_query.to_records():
    # for _, query in df_query.iloc[::-1].iterrows():
        print(query)
        execution_time = run_query(query)
        print(execution_time)
        time_list.append(execution_time)
        print("======================\n")

    df_query["execution_time"] = time_list
    current_time = time.strftime("%m%d-%H%M%S")
    df_query.to_csv(f"/home/uribe055/experiment-kit/experiment/results/vanilla_get_raster_changing_t_res_reverserun_result_{current_time}.csv", index=False)

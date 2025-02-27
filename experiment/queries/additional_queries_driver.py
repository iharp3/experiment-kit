import pandas as pd
import sys
import time
import os

main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))    # /../experiment-kit
sys.path.append(main_dir)

def run_query(q, r):
    start_time = time.time()
    if r == "heatmap":
        qe = HExecutor(
        variable=q["variable"],
        start_datetime=q["start_time"],
        end_datetime=q["end_time"],
        max_lat=q["max_lat"],
        min_lat=q["min_lat"],
        min_lon=q["min_lon"],
        max_lon=q["max_lon"],
        spatial_resolution=q["spatial_resolution"],
        aggregation=q["aggregation"],
        heatmap_aggregation_method=q["aggregation"],
    )
        

print("all modules loaded")

if __name__ == "__main__":
    systems_list = ["proposed", "vanilla"]
    query_list = ["heatmap", "find_time"]
    t_resolution_list = ["H", "Y"]
    s_resolution_list = [0.25, 1.0]
    time_span_list = [1, 2.5, 5, 10]

    for s in systems_list:
        if s == "proposed":
            from proposed.query_executor_heatmap import HeatmapExecutor as HExecutor
            from proposed.query_executor_find_time2 import FindTimeExecutor as FExecutor
        elif s == "vanilla":
            from vanilla.vanilla_get_heatmap_executor import VanillaGetHeatmapExecutor as HExecutor
            from vanilla.vanilla_find_time_executor import VanillaFindTimeExecutor as FExecutor
        else:   # s == "tiledb"
            from tile.tiledb_get_heatmap_executor import tiledb_get_heatmap_executor as HExecutor
            from tile.tiledb_find_time_executor import tiledb_find_time_executor as FExecutor

        # generate spatial region the size of alaska

        for t_res in t_resolution_list: # H, Y

            for s_res in s_resolution_list: # 0.25, 1.0


                for r in query_list:    # heatmap, find_time

                    for y in time_span_list:

                    if r == "heatmap":



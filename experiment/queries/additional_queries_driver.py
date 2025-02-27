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
    else: # r == "find_time"
        qe = FExecutor(
        variable= q["variable"],
        start_datetime=q["start_time"],
        end_datetime=q["end_time"],
        max_lat=q["max_lat"],
        min_lat=q["min_lat"],
        min_lon=q["min_lon"],
        max_lon=q["max_lon"],
        spatial_resolution=q["spatial_resolution"],
        temporal_resolution=q["temporal_resolution"],
        aggregation=q["aggregation"],
        filter_predicate=q["filter_predicate"],
        filter_value=q["filter_value"],
    )
    try:
        qe.execute()
    except Exception as e:
        print(q)
        print(e)
        return -1
    return time.time() - start_time
        

print("all modules loaded")

if __name__ == "__main__":
    
    system_list = ["proposed", "vanilla"]
    query_list = ["heatmap", "find_time"]

    for s in system_list:   # for each system
        for r in query_list:    # run heatmap and find time queries

            # load csv with queries
            if r == "heatmap":
                df_query = pd.read_csv("/home/uribe055/experiment-kit/experiment/queries/additional_heatmap_queries.csv")
            else:   # q == "find_time"
                df_query = pd.read_csv("/home/uribe055/experiment-kit/experiment/queries/additional_findtime_queries.csv")

            # load executors
            if s == "proposed":
                from proposed.query_executor_heatmap import HeatmapExecutor as HExecutor
                from proposed.query_executor_find_time2 import FindTimeExecutor as FExecutor
            elif s == "vanilla":
                from vanilla.vanilla_get_heatmap_executor import VanillaGetHeatmapExecutor as HExecutor
                from vanilla.vanilla_find_time_executor import VanillaFindTimeExecutor as FExecutor
            else:   # s == "tiledb"
                from tile.tiledb_get_heatmap_executor import tiledb_get_heatmap_executor as HExecutor
                from tile.tiledb_find_time_executor import tiledb_find_time_executor as FExecutor

            # run queries
            time_list = []
            for query in df_query.to_records():
                print(f"SYSTEM {s}:\t {query}")
                execution_time = run_query(q=query, r=r)
                print(execution_time)
                time_list.append(execution_time)
                print("======================\n")

            # save queries for each
            df_query["execution_time"] = time_list
            current_time = time.strftime("%m%d-%H%M%S")
            out_path = os.path.join(main_dir, "experiment/queries", f"{s}_additional_{r}_result_{current_time}.csv")
            df_query.to_csv(out_path, index=False)

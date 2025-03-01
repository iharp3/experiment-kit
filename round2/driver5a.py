import time
import pandas as pd
import os 
import sys

main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# print(main_dir)
sys.path.append(os.path.join(main_dir, "round2/executors"))

# impact of spatial resolution

cur_plot = [[0.25, "hour"], [0.25, "year"], [0.5, "month"], [1, "hour"], [1, "year"]]

# sys_list = ["TDB"]
sys_list = ["Vanilla", "Polaris"]

df_query = pd.read_csv(os.path.join(main_dir, f"round2/tests/5a.csv"))

results_list = []

for cur_sys in sys_list:    # p, v, t

    # load executors
    if cur_sys == "Polaris":
        from proposed.query_executor_get_raster import GetRasterExecutor as GRExecutor
        from proposed.query_executor_heatmap import HeatmapExecutor as HExecutor
        from proposed.query_executor_find_time2 import FindTimeExecutor as FExecutor
    elif cur_sys == "Vanilla":
        from vanilla.vanilla_get_raster_executor import VanillaGetRasterExecutor as GRExecutor
        from vanilla.vanilla_get_heatmap_executor import VanillaGetHeatmapExecutor as HExecutor
        from vanilla.vanilla_find_time_executor import VanillaFindTimeExecutor as FExecutor
    elif cur_sys == "TDB":   # s == "tiledb"
        from tiledb2.tiledb_get_raster_executor import tiledb_get_raster_executor as GRExecutor
        from tiledb2.tiledb_get_heatmap_executor import tiledb_get_heatmap_executor as HExecutor
        from tiledb2.tiledb_find_time_executor import tiledb_find_time_executor as FExecutor
    else:
        print("Unknown system")
        exit


    for p in cur_plot:  # (0.25, h) (0.25, y)...
        s = p[0]
        t = p[1]

        for q in df_query.to_records():

            qe = GRExecutor(
            variable=q["variable"],
            start_datetime=q["start_time"],
            end_datetime=q["end_time"],
            max_lat=q["max_lat"],
            min_lat=q["min_lat"],
            min_lon=q["min_lon"],
            max_lon=q["max_lon"],
            spatial_resolution=s,
            temporal_resolution=t,
            aggregation=q["aggregation"],
            )
            try:
                tr = qe.execute()
                print(f"s: {s}  t: {t}, q: {q}")
                print(tr)
            except Exception as e:
                print(q)
                print(e)
                tr = -1

            if tr != -1:
                if cur_sys == "Polaris":
                    ta = 0
                else:
                    ta = qe.agg()
                print(f"total time: {tr+ta}")
                results_list.append({"sys": cur_sys, 
                                        "t_res": t,
                                        "s_res": s,
                                        "percent_area": q["percent_area"],
                                        "tr": tr,
                                        "ta": ta,
                                        "total_time": tr + ta})
                print("======================\n")
            else:
                print(f"-1")

results_df = pd.DataFrame(results_list)
out_file = os.path.join(main_dir, f"round2/results/5a_{cur_sys}_results.csv")
results_df.to_csv(out_file, index=False)
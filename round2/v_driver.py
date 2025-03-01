
import time
import pandas as pd
import os 
import sys

main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# print(main_dir)
sys.path.append(os.path.join(main_dir, "round2/executors"))

t_res = ["hour"]
s_res = [0.25]
# sys_list = ["TDB"]
sys_list = ["Vanilla"]

df_query = pd.read_csv(os.path.join(main_dir, f"round2/tests/5c.csv"))
# run three of vanilla
rows =[["hour", 0.25, "2m_temperature","2015-01-01 00:00:00","2019-12-31 23:00:00",-76,-51,105,165,"min"],["hour", 0.25, "2m_temperature","2015-01-01 00:00:00","2019-12-31 23:00:00",-58,-33,60,120,"max"],["hour", 0.25, "2m_temperature","2015-01-01 00:00:00","2019-12-31 23:00:00",-53,-28,102,162,"mean"]]


results_list = []

for cur_sys in sys_list:

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


    for t in t_res:
        for s in s_res:

            for q in rows:  

                # ["hour", 0.25, "2m_temperature","2015-01-01 00:00:00","2019-12-31 23:00:00",-76,-51,105,165,"min"]

                qe = GRExecutor(
                variable=q[2],
                start_datetime=q[3],
                end_datetime=q[4],
                max_lat=q[6],
                min_lat=q[5],
                min_lon=q[7],
                max_lon=q[8],
                spatial_resolution=q[1],
                temporal_resolution=q[0],
                aggregation=q[9],
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
                                        "tr": tr,
                                        "ta": ta,
                                        "total_time": tr + ta})
                    print("======================\n")
                else:
                    print(f"-1")

results_df = pd.DataFrame(results_list)
out_file = os.path.join(main_dir, f"round2/results/5c_{cur_sys}_results_fixed.csv")
results_df.to_csv(out_file, index=False)
import time
import pandas as pd
import os 
import sys

main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# print(main_dir)
sys.path.append(os.path.join(main_dir, "round2/executors"))

# impact of spatial resolution

cur_plot = [[0.25, "hour"], [0.25, "year"], [0.5, "month"], [1, "hour"], [1, "year"]]

sys_list = ["TDB"]
# sys_list = ["Polaris", "Vanilla"]

df_query = pd.read_csv(os.path.join(main_dir, f"round2/tests/fv.csv"))

heatmap_results_list = []
find_time_results_list = []

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
        exit()


    for p in cur_plot:  # (0.25, h) (0.25, y)...
        s = p[0]    # s_res
        t = p[1]    # t_res

        for q in df_query.to_records():

            fqe = FExecutor(   
                variable=q["variable"],
                start_datetime=q["start_time"],
                end_datetime=q["end_time"],
                max_lat=q["max_lat"],
                min_lat=q["min_lat"],
                min_lon=q["min_lon"],
                max_lon=q["max_lon"],
                spatial_resolution=s,
                temporal_resolution=t,
                time_series_aggregation_method=q["aggregation"],
                aggregation=q["aggregation"],
                filter_predicate=q["filter_predicate"],
                filter_value=q["filter_value"],
                )

            try:
                t0 = time.time()
                fqe.execute()
                ftr = time.time() - t0
            except Exception as e:
                print("FIND TIME QUERY FAIL")
                print(q)
                print(e)
                ftr = -1
            
            print(f"s: {s}  t: {t}, q: {q}\ntr: {ftr}")


            # running find time query
            if ftr != -1:
                if cur_sys == "Polaris":
                    fta = 0
                else:
                    fta = 0
                print(f"FIND TIME total time: {ftr+fta}")
                find_time_results_list.append({"sys": cur_sys, 
                                        "t_res": t,
                                        "s_res": s,
                                        "filter_value": q["filter_value"],
                                        "tr": ftr,
                                        "ta": fta,
                                        "total_time": ftr + fta})
                print("======================\n")
            else:
                print(f"-1")


            in_find_time_results_df = pd.DataFrame(find_time_results_list)
            in_find_time_out_file = os.path.join(main_dir, f"round2/figs/f1_test/in_filter_value_{cur_sys}_results.csv")
            in_find_time_results_df.to_csv(in_find_time_out_file, mode='a', header=not os.path.exists(in_find_time_out_file), index=False)

find_time_results_df = pd.DataFrame(find_time_results_list)
find_time_out_file = os.path.join(main_dir, f"round2/results/filter_value_{cur_sys}_results.csv")
find_time_results_df.to_csv(find_time_out_file, index=False)
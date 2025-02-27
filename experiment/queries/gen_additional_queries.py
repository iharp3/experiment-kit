import pandas as pd
import numpy as np
import sys
import os

main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))    # /../experiment-kit
sys.path.append(main_dir)

from experiment.heatmap.gen_heatmap_queries import (gen_random_spatial_range, 
                                                    make_query as h_make_query)

from experiment.find_time.gen_find_time_queries import (make_query as e_make_query)

def gen_random_time_span(n_years, s_year, e_year):

    int_years = int(n_years)
    start_year = np.random.randint(s_year, e_year - int_years + 1)
    end_year = start_year + int_years
    start_time = f"{start_year}-01-01 00:00:00"
    if n_years % 1 == 0.5:
        end_time = f"{end_year}-06-30 23:00:00"
    else:
        end_time = f"{end_year-1}-12-31 23:00:00"
    return start_time, end_time

if __name__ == "__main__":

    queries = []

    query_list = ["heatmap", "find_time"]
    t_resolution_list = ["hour", "year"]
    s_resolution_list = [0.25, 1.0]
    time_span_list = [1, 2.5, 5, 10]
    agg_list = ["min", "max", "mean"]
    filter_values_list = [205, 240, 275]
    predicates_list = [">", "<"]

    area_size = 100
    fixed_filter_value = 310

    # generate spatial region the size of alaska
    max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(20, 50)

    for t_res in t_resolution_list: # H, Y
        for s_res in s_resolution_list: # 0.25, 1.0
            if s_res == 0.25 and t_res == "hour":
                continue
            for r in query_list:    # heatmap, find_time
                for y in time_span_list:  # 1 year, 2.5 years, 5 years, 10 years

                    if y == 10:
                        s_year = 2004
                        e_year = 2023
                    else:   # y = 1 year, 2.5 years, 5 years
                        s_year = 2014
                        e_year = 2023

                    for agg in agg_list:    # min, max, mean

                        # generate random time span of length y
                        # start_time, end_time = gen_random_time_span(n_years=y, s_year=s_year, e_year=e_year)

                        if r == "find_time":
                            if y == 5:
                                for pred in predicates_list:    # <, >
                                    for f in filter_values_list:    # 205, 240, 275
                                        start_time, end_time = gen_random_time_span(n_years=y, s_year=s_year, e_year=e_year)
                                        # generate queries for different values
                                        query = e_make_query(start_time, end_time, min_lat=60, max_lat=60, min_lon=-70, max_lon=-10, 
                                                                s_res=s_res, t_res=t_res, agg=agg, pred=pred, value=f)
                                        query["time_span"] = y
                                        query["area_persent"] = area_size
                                        query["category"] = "changing_value"
                                        queries.append(query)
                            else:
                                for pred in predicates_list:
                                    start_time, end_time = gen_random_time_span(n_years=y, s_year=s_year, e_year=e_year)
                                    query = e_make_query(start_time, end_time, min_lat=60, max_lat=60, min_lon=-70, max_lon=-10, 
                                                s_res=s_res, t_res=t_res, agg=agg, pred=pred, value=fixed_filter_value)
                                    query["time_span"] = y
                                    query["area_persent"] = area_size
                                    query["category"] = "changing_time"
                                    queries.append(query)

                        else:   # r == "heatmap"
                            start_time, end_time = gen_random_time_span(n_years=y, s_year=s_year, e_year=e_year)
                            query = h_make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, s_res, t_res, agg)
                            query["time_span"] = y
                            query["area_persent"] = area_size
                            query["category"] = "changing_time"
                            queries.append(query)
                            
    df = pd.DataFrame(queries)
    df["qid"] = df.index
    df = df[["qid"] + [col for col in df.columns if col != "qid"]]
    df.to_csv("/home/uribe055/experiment-kit/experiment/queries/additional_queries.csv", index=False)
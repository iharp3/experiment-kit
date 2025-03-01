import numpy as np
import pandas as pd
import os
import random

def gen_random_spatial_range(n_lat, n_lon):
    min_lat = np.random.randint(-90, 90 - n_lat)
    min_lon = np.random.randint(-180, 180 - n_lon)
    max_lat = min_lat + n_lat
    max_lon = min_lon + n_lon
    return max_lat, min_lat, min_lon, max_lon

def get_filter_predicate():
    num = random.choice([1, 2])
    if num == 1:
        return ">"
    else:
        return "<"

def gen_random_time_span_tiledb(n_years):
    if n_years > 5:
        n_years = 5
    start_year = np.random.randint(2014, 2020 - n_years + 1)
    end_year = start_year + n_years - 1
    start_time = f"{start_year}-01-01 00:00:00"
    end_time = f"{end_year}-12-31 23:00:00"
    return start_time, end_time

def gen_random_time_span_half_years(n_years):
    if n_years == 10:   # 1, 2.5, 5, 10
        int_years = int(n_years)
        start_year = np.random.randint(2004, 2023 - int_years + 1)  # for vanilla, polaris (tdb filtered out in driver)
    else:
        int_years = int(n_years)
        start_year = np.random.randint(2014, 2020 - int_years + 1)  # for tiledb
    end_year = start_year + int_years
    start_time = f"{start_year}-01-01 00:00:00"
    if n_years % 1 == 0.5:
        end_time = f"{end_year}-06-30 23:00:00"
    else:
        end_time = f"{end_year-1}-12-31 23:00:00"
    return start_time, end_time

def make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, s_res, t_res, agg):
    query = {
        "variable": "2m_temperature",
        "start_time": start_time,
        "end_time": end_time,
        "min_lat": min_lat,
        "max_lat": max_lat,
        "min_lon": min_lon,
        "max_lon": max_lon,
        "aggregation": agg,
    }
    return query

# generates queries for find time filter
if __name__ == "__main__":

    queries = []
    area = [25, 60]
    t_span = 5
    filter_value_list = [205, 240, 245, 255, 265, 275, 285, 295, 310] * 3
    max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(area[0], area[1])
    start_time, end_time = gen_random_time_span_half_years(t_span)

    for filter_value in filter_value_list:
        query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, 1, "day", "min")
        query["filter_predicate"] = ">"
        query["filter_value"] = filter_value

        queries.append(query)

    query_df = pd.DataFrame(queries)
    main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    out_file = "/home/uribe055/experiment-kit/round2/tests/fv_new.csv"
    query_df.to_csv(out_file)

# # generates queries for find time time span
# if __name__ == "__main__":

#     queries = []
#     aggs = ["min", "max", "mean"]

#     area = [25, 60]
#     time_span_list = [1, 2.5, 5.0, 10]
#     for t_span in time_span_list:
#         for agg in aggs:
#             max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(area[0], area[1])
#             start_time, end_time = gen_random_time_span_half_years(t_span)
#             query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, 1, "day", agg)

#             query["filter_predicate"] = get_filter_predicate()
#             query["filter_value"] = 310
#             query["time_span"] = t_span

#             queries.append(query)

#     query_df = pd.DataFrame(queries)
#     main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

#     out_file = "/home/uribe055/experiment-kit/round2/tests/ft.csv"
#     query_df.to_csv(out_file)

# # generates queries for heatmap query
# if __name__ == "__main__":

#     queries = []
#     aggs = ["min", "max", "mean"]

#     area = [25, 60]
#     time_span_list = [1, 2.5, 5.0, 10]
#     for t_span in time_span_list:
#         for agg in aggs:
#             max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(area[0], area[1])
#             start_time, end_time = gen_random_time_span_half_years(t_span)
#             query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, 1, "day", agg)

#             query["time_span"] = t_span

#             queries.append(query)

#     query_df = pd.DataFrame(queries)
#     main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

#     out_file = "/home/uribe055/experiment-kit/round2/tests/hmft.csv"
#     query_df.to_csv(out_file)


# # generates queries for 5b
# if __name__ == "__main__":
#     queries = []
#     aggs = ["min", "max", "mean"]

#     area = [25, 60]
#     time_span_list = [1, 2.5, 5.0, 10]
#     for t_span in time_span_list:
#         for agg in aggs:
#             max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(area[0], area[1])
#             start_time, end_time = gen_random_time_span_tiledb(5)
#             query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, 1, "day", agg)

#             query["time_span"] = t_span

#             queries.append(query)

#     query_df = pd.DataFrame(queries)
#     main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
#     # print(main_dir)
#     # out_file = os.path.join(main_dir, "/round2/tests/5a.csv")
#     out_file = "/data/experiment-kit/round2/tests/5b.csv"
#     query_df.to_csv(out_file)

# generates queries for 5a
# if __name__ == "__main__":
#     queries = []
#     aggs = ["min", "max", "mean"]

#     area_percents = [[3, 5], [15, 25], [25, 30], [25, 60]]
#     percent_list = [1, 25, 50, 100]
#     for area, per_cent in zip(area_percents, percent_list):
#         for agg in aggs:
#             max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(area[0], area[1])
#             start_time, end_time = gen_random_time_span_tiledb(5)
#             query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, 1, "day", agg)

#             query["percent_area"] = per_cent

#             queries.append(query)

#     query_df = pd.DataFrame(queries)
#     main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
#     # print(main_dir)
#     # out_file = os.path.join(main_dir, "/round2/tests/5a.csv")
#     out_file = "/data/experiment-kit/round2/tests/5a.csv"
#     query_df.to_csv(out_file)


 # generates queries for 5c, 5d
# if __name__ == "__main__":
#     queries = []
#     aggs = ["min", "max", "mean"]

#     area = [25, 60]

#     for agg in aggs:
#         max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(area[0], area[1])
#         start_time, end_time = gen_random_time_span_tiledb(5)
#         query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, 1, "day", agg)
#         queries.append(query)

#     query_df = pd.DataFrame(queries)
#     out_file = "/data/experiment-kit/round2/tests/5d.csv"   # 5c.csv
#     query_df.to_csv(out_file)

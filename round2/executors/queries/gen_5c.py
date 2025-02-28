import numpy as np
import pandas as pd

def gen_random_spatial_range(n_lat, n_lon):
    min_lat = np.random.randint(-90, 90 - n_lat)
    min_lon = np.random.randint(-180, 180 - n_lon)
    max_lat = min_lat + n_lat
    max_lon = min_lon + n_lon
    return max_lat, min_lat, min_lon, max_lon

def gen_random_time_span_tiledb(n_years):
    if n_years > 5:
        n_years = 5
    start_year = np.random.randint(2014, 2020 - n_years + 1)
    end_year = start_year + n_years - 1
    start_time = f"{start_year}-01-01 00:00:00"
    end_time = f"{end_year}-12-31 23:00:00"
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

# generates queries for 5a
if __name__ == "__main__":
    queries = []
    aggs = ["min", "max", "mean"]

    area_percents = [[3, 5], [15, 25], [25, 30], [25, 60]]
    for area in area_percents:
        for agg in aggs:
            max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(area[0], area[1])
            start_time, end_time = gen_random_time_span_tiledb(5)
            query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, 1, "day", agg)
            queries.append(query)

    query_df = pd.DataFrame(queries)
    out_file = "/data/experiment-kit/round2/tests/5a.csv" 
    query_df.to_csv(out_file)


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

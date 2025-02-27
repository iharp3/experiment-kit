import numpy as np
import pandas as pd

# Alaska
# Latitude: 51.25 to 71.38, approximately 20 degrees
# Longitude: -179.15 to -129.98, approximately 50 degrees

# Greenland
# Latitude: 59.79 to 83.63, approximately 25 degrees
# Longitude: -73.00 to -12.00, approximately 60 degrees


def gen_random_spatial_range(n_lat, n_lon):
    min_lat = np.random.randint(-90, 90 - n_lat)
    min_lon = np.random.randint(-180, 180 - n_lon)
    max_lat = min_lat + n_lat
    max_lon = min_lon + n_lon
    return max_lat, min_lat, min_lon, max_lon


def gen_random_time_span(n_years):
    if n_years == 20:
        return "2004-01-01 00:00:00", "2023-12-31 23:00:00"

    int_years = int(n_years)
    start_year = np.random.randint(2004, 2023 - int_years + 1)
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
        "spatial_resolution": s_res,
        "temporal_resolution": t_res,
        "aggregation": agg,
    }
    return query


def figure_5_queries():
    queries = []
    aggs = ["min", "max", "mean"]
    for s_res in [0.25, 1]:
        for t_res in ["hour", "year"]:
            # 1. change area, 5 years
            areas = [[3, 5], [15, 25], [25, 30], [25, 60]]
            for n_lat, n_lon in areas:
                for agg in aggs:
                    max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(n_lat, n_lon)
                    start_time, end_time = gen_random_time_span(5)
                    query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, s_res, t_res, agg)
                    query["time_span"] = 5
                    query["area_persent"] = int((n_lat * n_lon) / (25 * 60) * 100)
                    query["category"] = "changing_area"
                    queries.append(query)

            # 2. 100%, change years
            years = [1, 2.5, 5, 10]
            for year in years:
                for agg in aggs:
                    max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(25, 60)
                    start_time, end_time = gen_random_time_span(year)
                    query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, s_res, t_res, agg)
                    query["time_span"] = year
                    query["area_persent"] = 100
                    query["category"] = "changing_time"
                    queries.append(query)
    return queries


def figure_6_queries():
    queries = []
    aggs = ["min", "max", "mean"]
    for t_res in ["hour", "day", "month", "year"]:
        for s_res in [0.25, 0.5, 1]:
            # 3. 100%, 5 years, change resolution
            for agg in aggs:
                max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(25, 60)
                start_time, end_time = gen_random_time_span(5)
                query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, s_res, t_res, agg)
                query["time_span"] = 5
                query["area_persent"] = 100
                query["category"] = "changing_resolution"
                queries.append(query)
    return queries


def check_same_start_year(queries):
    last_start_year = None
    for query in queries:
        start_year = query["start_time"].split("-")[0]
        if last_start_year is not None and last_start_year == start_year:
            return True
        last_start_year = start_year
    return False


if __name__ == "__main__":
    queries = figure_5_queries() + figure_6_queries()
    while check_same_start_year(queries):
        queries = figure_5_queries() + figure_6_queries()

    df = pd.DataFrame(queries)
    df["qid"] = df.index
    df = df[["qid"] + [col for col in df.columns if col != "qid"]]
    df.to_csv("GR_test_0227.csv", index=False)

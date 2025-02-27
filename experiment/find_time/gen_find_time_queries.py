import numpy as np
import pandas as pd

# Alaska
# Latitude: 51.25 to 71.38, approximately 20 degrees
# Longitude: -179.15 to -129.98, approximately 50 degrees

# Greenland
# Latitude: 59.79 to 83.63, approximately 25 degrees
# Longitude: -73.00 to -12.00, approximately 60 degrees


variable = "2m_temperature"
temporal_resolution = "hour"
max_lat = 85
min_lat = 60
min_lon = -70
max_lon = -10
spatial_resolution = 0.25
aggregation = "mean"
time_series_aggregation_method = "mean"


def gen_random_time_span(n_years):
    if n_years == 20:
        return "2004-01-01 00:00:00", "2023-12-31 23:00:00"

    int_years = int(n_years)
    start_year = np.random.randint(2014, 2023 - int_years + 1)  # for tiledb
    # start_year = np.random.randint(2004, 2023 - int_years + 1)  # for vanilla, polaris
    end_year = start_year + int_years
    start_time = f"{start_year}-01-01 00:00:00"
    if n_years % 1 == 0.5:
        end_time = f"{end_year}-06-30 23:00:00"
    else:
        end_time = f"{end_year-1}-12-31 23:00:00"
    return start_time, end_time


def make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, s_res, t_res, agg, pred, value):
    return {
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
        "time_series_aggregation_method": agg,
        "filter_predicate": pred,
        "filter_value": value,
    }


def gen_queries():
    queries = []
    aggs = ["min", "max", "mean"]
    predicates = [">", "<"]
    values = [205, 240, 275, 310]
    # spans = [
    #     [1, "2010-01-01 00:00:00", "2010-12-31 23:00:00"],  # 1 year
    #     [2.5, "2011-01-01 00:00:00", "2013-06-30 23:00:00"],  # 2.5 years
    #     [5, "2014-01-01 00:00:00", "2018-12-31 23:00:00"],  # 5 years
    #     [10, "2010-01-01 00:00:00", "2019-12-31 23:00:00"],  # 10 years
    # ]

    # 1. [1, 2.5, 5, 10 years], pred 310
    # for span in [1, 2.5, 5, 10]:
    #     for agg in aggs:
    #         for pred in predicates:
    #             start_time, end_time = gen_random_time_span(span)
    #             query = make_query(
    #                 start_time, end_time, min_lat, max_lat, min_lon, max_lon, 0.25, "hour", agg, pred, 310
    #             )
    #             query["time_span"] = span
    #             query["category"] = "changing_time"
    #             queries.append(query)

    # 2. 10 years, pred [205, 240, 275, 310]
    for value in values:
        for agg in aggs:
            for pred in predicates:
                start_time, end_time = gen_random_time_span(5)
                query = make_query(
                    start_time, end_time, min_lat, max_lat, min_lon, max_lon, 0.25, "hour", agg, pred, value
                )
                query["time_span"] = 5
                query["category"] = "changing_value"
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
    queries = gen_queries()
    while check_same_start_year(queries):
        queries = gen_queries()

    df = pd.DataFrame(queries)
    df["qid"] = df.index
    df = df[["qid"] + [col for col in df.columns if col != "qid"]]
    df.to_csv("/data/experiment-kit/experiment/find_time/findtime_test_5yr.csv", index=False)

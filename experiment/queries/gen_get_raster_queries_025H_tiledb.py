import numpy as np
import pandas as pd

# Alaska
# Latitude: 51.25 to 71.38, approximately 20 degrees
# Longitude: -179.15 to -129.98, approximately 50 degrees

# Greenland
# Latitude: 59.79 to 83.63, approximately 25 degrees
# Longitude: -73.00 to -12.00, approximately 60 degrees


def gen_random_spatial_range(n_lat, n_lon):
    # print(f"\t\t\tgetting random spatial range...")
    min_lat = np.random.randint(-90, 90 - n_lat)
    min_lon = np.random.randint(-180, 180 - n_lon)
    max_lat = min_lat + n_lat
    max_lon = min_lon + n_lon
    # print(f"\t\t\t\t\t\treturning... {max_lat, min_lat, min_lon, max_lon}")
    return max_lat, min_lat, min_lon, max_lon


def gen_random_time_span(n_years):
    # print(f"\t\t\tgetting random timespan...")
    if n_years > 5:
        n_years = 5
    int_years = int(n_years)
    start_year = np.random.randint(2014, 2020 - int_years + 1)
    end_year = start_year + int_years
    start_time = f"{start_year}-01-01 00:00:00"
    if n_years % 1 == 0.5:
        end_time = f"{end_year}-06-30 23:00:00"
    else:
        end_time = f"{end_year-1}-12-31 23:00:00"
    # print(f"\t\t\t\t\t\treturning {start_time, end_time}...")
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


def gen_queries():
    queries = []
    aggs = ["min", "max", "mean"]

    # 1. [10%, 25%, 50%, 100% of Greenland], 5 years, 0.25, hourly
    print("f\nChanging area")
    areas = [[3, 5], [15, 25], [25, 30], [25, 60]]
    for n_lat, n_lon in areas:
        # print(f"\t({n_lat, n_lon})")
        for agg in aggs:
            # print(f"\t\tagg: {agg}")
            max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(n_lat, n_lon)
            start_time, end_time = gen_random_time_span(5)
            query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, 0.25, "hour", agg)
            query["time_span"] = 5
            query["area_persent"] = int((n_lat * n_lon) / (25 * 60) * 100)
            query["category"] = "changing_area"
            queries.append(query)

    # 2. Greenland, [1, 2.5, 5, 10 years], 0.25, hourly
    print(f"\nChanging time")
    years = [1, 2.5, 5]
    for year in years:
        # print(f"\t{year}")
        for agg in aggs:
            # print(f"\t\tagg: {agg}")
            max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(25, 60)
            start_time, end_time = gen_random_time_span(year)
            query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, 0.25, "hour", agg)
            query["time_span"] = year
            query["area_persent"] = 100
            query["category"] = "changing_time"
            queries.append(query)

    # 3. Greenland, 5 years, [0.25, 0.5, 1], hourly
    print(f"\nChaning spatial resolution")
    spatial_resolutions = [0.25, 0.5, 1]
    for s_res in spatial_resolutions:
        # print(f"\t{s_res}")
        for agg in aggs:
            # print(f"\t\tagg: {agg}")
            max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(25, 60)
            start_time, end_time = gen_random_time_span(5)
            query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, s_res, "hour", agg)
            query["time_span"] = 5
            query["area_persent"] = 100
            query["category"] = "changing_spatial_res"
            queries.append(query)

    # 4. Greenland, 5 years, 0.25, [hourly, daily, monthly, yearly]
    print(f"\nChaning temporal resolution")
    temporal_resolutions = ["hour", "day", "month", "year"]
    for t_res in temporal_resolutions:
        # print(f"\t{t_res}")
        for agg in aggs:
            # print(f"\t\tagg: {agg}")
            max_lat, min_lat, min_lon, max_lon = gen_random_spatial_range(25, 60)
            start_time, end_time = gen_random_time_span(5)
            query = make_query(start_time, end_time, min_lat, max_lat, min_lon, max_lon, 0.25, t_res, agg)
            query["time_span"] = 5
            query["area_persent"] = 100
            query["category"] = "changing_temporal_res"
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
    # while check_same_start_year(queries):
    #     print(f"\n######### SAME START YEAR #########")
    # queries = gen_queries()

    df = pd.DataFrame(queries)
    df["qid"] = df.index
    df = df[["qid"] + [col for col in df.columns if col != "qid"]]
    df.to_csv("/data/experiment-kit/experiment/queries/get_raster_test_set_025H_tiledb.csv", index=False)
from cloud_get_timeseries_executor import cloud_get_timeseries_executor


class cloud_find_time_executor:
    def __init__(
        self,
        variable: str,
        start_datetime: str,
        end_datetime: str,
        temporal_resolution: str,  # "hour", "day", "month", "year"
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        spatial_resolution: float,  # 0.25, 0.5, 1
        aggregation: str,  # "mean", "max", "min"  !! Use this aggregation for all aggregation
        filter_predicate: str,  # "<", ">" !! only these two predicates are enough
        filter_value: float,
    ):
        # change min_lon, max_lon to 0-360
        min_lon = min_lon % 360
        max_lon = max_lon % 360

        self.variable = variable
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.temporal_resolution = temporal_resolution
        self.min_lat = min_lat
        self.max_lat = max_lat
        self.min_lon = min_lon
        self.max_lon = max_lon
        self.spatial_resolution = spatial_resolution
        self.aggregation = aggregation
        self.filter_predicate = filter_predicate
        self.filter_value = filter_value

    def execute(self):
        timeseries_executor = cloud_get_timeseries_executor(
            self.variable,
            self.start_datetime,
            self.end_datetime,
            self.temporal_resolution,
            self.min_lat,
            self.max_lat,
            self.min_lon,
            self.max_lon,
            self.spatial_resolution,
            self.aggregation,
        )
        ts = timeseries_executor.execute()
        if self.filter_predicate == ">":
            res = ts.where(ts > self.filter_value, drop=False)
        elif self.filter_predicate == "<":
            res = ts.where(ts < self.filter_value, drop=False)
        else:
            raise ValueError("Invalid filter_predicate")
        res = res.fillna(False)
        res = res.astype(bool)
        return res

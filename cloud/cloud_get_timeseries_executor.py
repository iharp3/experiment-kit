from cloud_get_raster_executor import cloud_get_raster_executor


class cloud_get_timeseries_executor:
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
        aggregation: str,  # "mean", "max", "min"  !! Use this aggregation for timeseries aggregation as well
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

    def execute(self):
        get_raster_exectuor = cloud_get_raster_executor(
            variable=self.variable,
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
            temporal_resolution=self.temporal_resolution,
            min_lat=self.min_lat,
            max_lat=self.max_lat,
            min_lon=self.min_lon,
            max_lon=self.max_lon,
            spatial_resolution=self.spatial_resolution,
            aggregation=self.aggregation,
        )

        raster = get_raster_exectuor.execute()

        if self.aggregation == "mean":
            time_series = raster.mean(dim=["latitude", "longitude"])
        elif self.aggregation == "max":
            time_series = raster.max(dim=["latitude", "longitude"])
        elif self.aggregation == "min":
            time_series = raster.min(dim=["latitude", "longitude"])
        else:
            raise ValueError(f"Invalid aggregation method: {self.aggregation}")

        return time_series.compute()

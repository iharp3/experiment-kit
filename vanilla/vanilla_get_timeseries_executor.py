import xarray as xr
from vanilla_get_raster_executor import VanillaGetRasterExecutor

class VanillaGetTimeseriesExecutor:
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
        vanilla_get_raster_executor = VanillaGetRasterExecutor(
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
        vanilla_raster = vanilla_get_raster_executor.execute()
        if self.aggregation == "mean":
            return vanilla_raster.mean(dim=["latitude", "longitude"]).compute()
        elif self.aggregation == "max":
            return vanilla_raster.max(dim=["latitude", "longitude"]).compute()
        elif self.aggregation == "min":
            return vanilla_raster.min(dim=["latitude", "longitude"]).compute()
        else:
            raise ValueError(f"Invalid time series aggregation method: {self.aggregation}")

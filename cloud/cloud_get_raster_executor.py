import xarray as xr


def temporal_resolution_to_freq(resolution):
    if resolution == "hour":
        return "h"
    elif resolution == "day":
        return "D"
    elif resolution == "month":
        return "ME"
    elif resolution == "year":
        return "YE"
    else:
        raise ValueError("Invalid temporal_resolution")


class CloudGetRasterExecutor:

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
        aggregation: str,  # "mean", "max", "min"
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
        ar = xr.open_zarr(
            "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3"
        )
        da = ar[self.variable].sel(
            time=slice(self.start_datetime, self.end_datetime),
            latitude=slice(self.max_lat, self.min_lat),
            longitude=slice(self.min_lon, self.max_lon),
        )

        # temporal aggregation
        if self.temporal_resolution != "hour":
            resampled = da.resample(
                time=temporal_resolution_to_freq(self.temporal_resolution)
            )
            if self.aggregation == "mean":
                da = resampled.mean()
            elif self.aggregation == "max":
                da = resampled.max()
            elif self.aggregation == "min":
                da = resampled.min()
            else:
                raise ValueError(
                    f"Temporal aggregation {self.aggregation} is not supported."
                )

        # spatial aggregation
        if self.spatial_resolution > 0.25:
            c_f = int(self.spatial_resolution / 0.25)
            coarsened = da.coarsen(latitude=c_f, longitude=c_f, boundary="trim")
            if self.aggregation == "mean":
                da = coarsened.mean()
            elif self.aggregation == "max":
                da = coarsened.max()
            elif self.aggregation == "min":
                da = coarsened.min()
            else:
                raise ValueError(
                    f"Spatial aggregation {self.aggregation} is not supported."
                )

        return da.compute()

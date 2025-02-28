import tiledb

try:
    from tiledb2.utils import get_time_indices, get_spatial_range, nparray_to_xarray, temporal_resolution_to_freq
except:
    from utils import get_time_indices, get_spatial_range, nparray_to_xarray, temporal_resolution_to_freq


class tiledb_get_raster_executor:

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
        aggregation: str,  # "mean", "max", "min"  !! This aggregation applies for both temporal and spatial aggregation
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
        s, e = get_time_indices(start_time=self.start_datetime, end_time=self.end_datetime)
        max_la, min_la, max_lo, min_lo = get_spatial_range(
            max_lat=self.max_lat, min_lat=self.min_lat, max_lon=self.max_lon, min_lon=self.min_lon
        )

        tdb_nparray = None
        with tiledb.open("/data/iharp-customized-storage/storage/experiments_tdb", mode="r") as array:
            time_range = slice(s, e)
            lat_range = slice(min_la, max_la)
            lon_range = slice(min_lo, max_lo)
            tdb_nparray = array[time_range, lat_range, lon_range][self.variable]  # numpy array

        ds = nparray_to_xarray(
            tdb_nparray,
            self.start_datetime,
            self.end_datetime,
            self.min_lat,
            self.max_lat,
            self.min_lon,
            self.max_lon,
        )

        # temporal aggregation
        if self.temporal_resolution != "hour":
            resampled = ds.resample(time=temporal_resolution_to_freq(self.temporal_resolution))
            if self.aggregation == "mean":
                ds = resampled.mean()
            elif self.aggregation == "max":
                ds = resampled.max()
            elif self.aggregation == "min":
                ds = resampled.min()
            else:
                raise ValueError(f"Temporal aggregation {self.aggregation} is not supported.")

        # spatial aggregation
        if self.spatial_resolution > 0.25:
            c_f = int(self.spatial_resolution / 0.25)
            coarsened = ds.coarsen(latitude=c_f, longitude=c_f, boundary="trim")
            if self.aggregation == "mean":
                ds = coarsened.mean()
            elif self.aggregation == "max":
                ds = coarsened.max()
            elif self.aggregation == "min":
                ds = coarsened.min()
            else:
                raise ValueError(f"Spatial aggregation {self.aggregation} is not supported.")
        return ds.compute()

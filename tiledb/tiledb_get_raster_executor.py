import json
import tiledb

from utils import get_time_indices, get_spatial_range

json_file = "/data/experiment-kit/tiledb/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f),

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
        max_la, min_la, max_lo, min_lo = get_spatial_range(max_lat=self.max_lat,
                                                           min_lat=self.min_lat,
                                                           max_lon=self.max_lon,
                                                           min_lon=self.min_lon)

        with tiledb.open(inputs["tiledb_data_dir"], mode="r") as array:
            time_range = slice(s,e)         # Time indices (inclusive start, exclusive end)
            lat_range = slice(min_la, max_la)
            lon_range = slice(min_lo, max_lo)

            ds = array[time_range, lat_range, lon_range][self.variable]     # numpy array

        # temporal aggregation
        # TODO

        # spatial aggregation
        # TODO

        print(ds)
        return ds
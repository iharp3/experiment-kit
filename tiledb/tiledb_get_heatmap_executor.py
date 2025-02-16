import json
import numpy as np
import tiledb

from tiledb_get_raster_executor import get_time_indices, get_spatial_range

json_file = "/data/experiment-kit/tiledb/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f),

class tiledb_get_heatmap_executor:
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
        aggregation: str,  # "mean", "max", "min"  !! Use this aggregation for heatmap aggregation as well
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
        # define grid resolution for coarsening
        if self.spatial_resolution == 0.25:
            lat_block = lon_block = 1
        elif self.spatial_resolution == 0.5:
            lat_block = lon_block = 2
        elif self.spatial_resolution  == 1:
            lat_block = lon_block = 4
        else:
            return ValueError(f"Invalid spatial resolution {self.spatial_resolution}")

        # get indices for dataset slice
        s, e = get_time_indices(start_time=self.start_datetime, end_time=self.end_datetime)
        max_la, min_la, max_lo, min_lo = get_spatial_range(max_lat=self.max_lat,
                                                           min_lat=self.min_lat,
                                                           max_lon=self.max_lon,
                                                           min_lon=self.min_lon)

        lat_size, lon_size = ((max_la - min_la), (max_lo - min_lo))
        coarse_lat_size = lat_size // lat_block
        coarse_lon_size = lon_size // lon_block

        mean_temperature = np.zeros((lat_size, lon_size))

        with tiledb.open(inputs["tiledb_data_dir"], mode="r") as array:
            # get data slice you're interested in

            for lat in range(lat_size):
                for lon in range(lon_size):
                    mean_temperature[lat, lon] = array[:, lat, lon].agg(self.aggregation)[self.variable]

        # Reshape and average blocks
        coarse_mean_temperature = (
            mean_temperature[:coarse_lat_size * lat_block, :coarse_lon_size * lon_block]
            .reshape(coarse_lat_size, lat_block, coarse_lon_size, lon_block)
            .mean(axis=(1, 3))
        )

        print(coarse_mean_temperature.shape)

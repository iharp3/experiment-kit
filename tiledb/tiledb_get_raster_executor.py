import json
import numpy as np
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
        
        if self.temporal_resolution == "hour" and self.spatial_resolution == 0.25: 
            with tiledb.open(inputs["tiledb_data_dir"], mode="r") as array:
                time_range = slice(s,e)         # Time indices (inclusive start, exclusive end)
                lat_range = slice(min_la, max_la)
                lon_range = slice(min_lo, max_lo)
                coarse_data = array[time_range, lat_range, lon_range][self.variable]   # numpy array
        else:
            if self.aggregation == "mean":
                agg_function = np.mean
            elif self.aggregation == "max":
                agg_function = np.max
            elif self.aggregation == "min":
                agg_function = np.min
            else:
                return ValueError(f"Invalid aggregation {self.aggregation}")
            
            if self.spatial_resolution == 0.25:
                lat_block = lon_block = 1
            elif self.spatial_resolution == 0.5:
                lat_block = lon_block = 2
            elif self.spatial_resolution  == 1:
                lat_block = lon_block = 4
            else:
                return ValueError(f"Invalid spatial resolution {self.spatial_resolution}")
            
            if self.temporal_resolution == "hour":
                time_block = [i for i in range(s, e, 1)]
            elif self.temporal_resolution == "day":
                # TODO: make a list of increasing 24 indices from the start of indices and the end of indices 
                #       (careful to agg the same days together (if start idx starts mid day you have to do 12 and then 24...))
                time_block = []
            elif self.temporal_resolution == "month":
                # TODO: get index pairs (start_month, end_month) for every month in the time range 
                #       need y, m, d, h and then 
                #       for y: get monthly (start, end) indices for whole year (st or leap)
                #       for m: get monthly (start, end) indices for each specific months in the list
                #       for d: combine all the leftover day (24 indices to count per day) and hour values (one index per hour) into one
                time_block = []
            elif self.temporal_resolution == "year":
                # TODO: get index pairs of each year in the set
                # (max(start_time_idx, first yr start idx), first yr end idx), (middle yr start idx, middle yr end idx), (end yr start idx, min(end_time_idx, end yr end idx))
                time_block = []

            lat_size = max_la - min_la
            lon_size = max_lo - min_lo
            coarse_lat_size = lat_size // lat_block
            coarse_lon_size = lon_size // lon_block    
            coarse_time_size = len(time_block)

            coarse_data = np.zeros((coarse_time_size, coarse_lat_size, coarse_lon_size))

            with tiledb.open(inputs["tiledb_data_dir"], mode="r") as array:
                for t in time_block:
                    cur_start_time = t[0]
                    cur_end_time = t[1]
                    # Read data for the selected time block
                    temp_data = array[cur_start_time:cur_end_time, :,:][self.variable]  # Shape: (t, lat, lon)

                    aggregated_over_time = agg_function(temp_data, axis=0)  # Shape: (lat, lon)

                    coarse_data[cur_end_time, :, :] = (
                    agg_function(
                        aggregated_over_time[:coarse_lat_size * lat_block, :coarse_lon_size * lon_block]
                        .reshape(coarse_lat_size, lat_block, coarse_lon_size, lon_block),
                        axis=(1, 3)  # Aggregate over lat_block and lon_block
                    )
                )

        print(coarse_data.shape)
        return coarse_data
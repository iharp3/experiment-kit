import json
import numpy as np
import pandas as pd
import tiledb

from utils import get_time_indices, get_spatial_range, get_index_pairs

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
                time_block = [(i, i+1) for i in range(s, e + 1, 1)] # [inclusive, exclusive)
            else:
                timestamps = pd.period_range(start=self.start_datetime, end=self.end_datetime, freq="h")  # list of times within time range at specified resolution 
                time_block = get_index_pairs(timestamps=timestamps, time_res=self.temporal_resolution, start_time=self.start_datetime)
                
            lat_size = max_la - min_la
            lon_size = max_lo - min_lo
            coarse_lat_size = int(lat_size // lat_block)
            coarse_lon_size = int(lon_size // lon_block)    
            coarse_time_size = int(len(time_block))

            print(f"coarse lat: {coarse_lat_size}  coarse lon: {coarse_lon_size}  coarse time: {coarse_time_size}")
            print(f"lat block: {lat_block}  lon block: {lon_block}")
            print(f"coarse lat size * lat block = {coarse_lat_size * lat_block}")

            coarse_data = np.zeros((coarse_time_size, coarse_lat_size, coarse_lon_size))

            counter = 0
            with tiledb.open("/data/iharp-customized-storage/storage/experiments_tdb", mode="r") as array:
                print(f"\narray.shape: {array.shape}")
                for t in time_block:
                    cur_start_time = t[0]
                    cur_end_time = t[1]
                    # print(f"\nstart: {cur_start_time}   end: {cur_end_time} lat: {min_la, max_la}   lon: {min_lo, max_lo}")
                    # Read all desired lat/long data for the selected time block
                    temp_data = array[cur_start_time:cur_end_time, min_la:max_la, min_lo:max_lo][self.variable]  # Shape: (t, lat, lon)
                    # Agg over the whole time block
                    aggregated_over_time = agg_function(temp_data, axis=0)  # Shape: (lat, lon)
                    # Agg over space
                    coarse_data[counter, :, :] = (
                        agg_function(
                            aggregated_over_time[:coarse_lat_size * lat_block, :coarse_lon_size * lon_block]    # TODO: fix the lat and long aggregations
                            .reshape(coarse_lat_size, lat_block, coarse_lon_size, lon_block),
                            axis=(1, 3)  # Aggregate over lat_block and lon_block
                        )
                    )
                    counter+=1

        print(f"\n\t coarse_data shape: {coarse_data.shape}")
        return coarse_data
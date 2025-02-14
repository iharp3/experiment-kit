import numpy as np
import xarray as xr
import pandas as pd

from utils.const import long_short_name_dict
from cloud_get_raster_executor import cloud_get_raster_executor
from utils.get_whole_period import (
    get_whole_ranges_between,
    get_total_hours_in_year,
    get_total_hours_in_month,
    iterate_months,
    number_of_days_inclusive,
    number_of_hours_inclusive,
    get_total_hours_between,
)



class cloud_get_heatmap_executor:
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
        self.variable_short_name = long_short_name_dict[self.variable]


    def execute(self):
        if self.aggregation == "mean":
            return self._get_mean_heatmap()
        elif self.aggregation == "max":
            return self._get_max_heatmap()
        elif self.aggregation == "min":
            return self._get_min_heatmap()
        else:
            return ValueError(f"Invalid aggregation method {self.aggregation}")
        

    def _get_mean_heatmap(self):
        # hour_start = pd.Timestamp(f"{self.start_datetime} 00:00:00")
        # hour_end = pd.Timestamp(f"{self.end_datetime} 23:00:00")
        # hour_range =  [hour_start, hour_end]
        

        ds_hour = []
        # hour_hours = []
        
        get_raster = cloud_get_raster_executor(
            variable= self.variable,
            start_datetime= self.start_datetime,
            end_datetime= self.end_datetime,
            temporal_resolution= "hour",
            min_lat= self.min_lat,
            max_lat= self.max_lat,
            min_lon= self.min_lon,
            max_lon= self.max_lon,
            spatial_resolution= self.spatial_resolution,
            aggregation= self.aggregation,
        )
        ds_hour.append(get_raster.execute())
        hours_hours = [1 for _ in range(number_of_days_inclusive(self.start_datetime, self.end_datetime))]
        
        xrds_concat = xr.concat(ds_hour, dim="time")
        nd_array = xrds_concat.to_numpy()
        weights = np.array(hours_hours)
        total_hours = nd_array.shape[0]
        weights = np.ones(total_hours) / total_hours
        #weights = weights / total_hours
        average = np.average(nd_array, axis=0, weights=weights)
        res = xr.Dataset(
            {self.variable_short_name: (["latitude", "longitude"], average)},
            coords={"latitude": xrds_concat.latitude, "longitude": xrds_concat.longitude},
        )
        return res
    
    def _get_max_heatmap(self):
        hour_range = get_total_hours_between(
            self.start_datetime, self.end_datetime
        )

        ds_hour = []
    
        get_raster = cloud_get_raster_executor(
            variable= self.variable,
            start_datetime= self.start_datetime,
            end_datetime= self.end_datetime,
            temporal_resolution= "hour",
            min_lat= self.min_lat,
            max_lat= self.max_lat,
            min_lon= self.min_lon,
            max_lon= self.max_lon,
            spatial_resolution= self.spatial_resolution,
            aggregation= self.aggregation,
        )
        ds_hour.append(get_raster.execute())
        
        return xr.concat(ds_hour, dim="time").max(dim="time")
    
    def _get_min_heatmap(self):
        ds_hour = []
    
        get_raster = cloud_get_raster_executor(
            variable= self.variable,
            start_datetime= self.start_datetime,
            end_datetime= self.end_datetime,
            temporal_resolution= "hour",
            min_lat= self.min_lat,
            max_lat= self.max_lat,
            min_lon= self.min_lon,
            max_lon= self.max_lon,
            spatial_resolution= self.spatial_resolution,
            aggregation= self.aggregation,
        )
        ds_hour.append(get_raster.execute())
        
        return xr.concat(ds_hour, dim="time").min(dim="time")
import pandas as pd
import xarray as xr
import numpy as np

from cloud_get_raster_executor import cloud_get_raster_executor
from cloud_get_timeseries_executor import cloud_get_timeseries_executor
from utils.get_whole_period import get_whole_period_between, get_last_date_of_month, time_array_to_range

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
        ''''
        this check might be useless since it should all be hourly data
        but i was given temporal_resolution as a variable so I am going to leave it
        just in case it is needed.
        '''
        # if self.temporal_resolution == "hour" and self.filter_predicate != "!=":
        #     return self._execute_pyramid_hour()
        return self._execute_baseline()
    

    def execute_baseline(self):
        return self._execute_baseline()

    def _execute_baseline(self, start_datetime=None, end_datetime=None):
        if start_datetime is None:
            start_datetime = self.start_datetime
        if end_datetime is None:
            end_datetime = self.end_datetime
        timeseries_executor = cloud_get_timeseries_executor(
            self.variable,
            start_datetime,
            end_datetime,
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
        elif self.filter_predicate == "==":
            res = ts.where(ts == self.filter_value, drop=False)
        elif self.filter_predicate == "!=":
            res = ts.where(ts != self.filter_value, drop=False)
        elif self.filter_predicate == ">=":
            res = ts.where(ts >= self.filter_value, drop=False)
        elif self.filter_predicate == "<=":
            res = ts.where(ts <= self.filter_value, drop=False)
        else:
            raise ValueError("Invalid filter predicate")
        
        res = res.fillna(False)
        res = res.astype(bool)
        return res

    # def _execute_pyramid_hour(self):
    #     time_points = pd.date_range(start=self.start_datetime, end=self.end_datetime, freq="h")
    #     result = xr.Dataset(
    #         data_vars={self.variable: (["valid_time"], [None] * len(time_points))},
    #         coords=dict(valid_time=time_points),
    #     )
        
    #     result_undetermined = result["valid_time"].where(result["2m_temperature"].isnull(), drop=True)
    #     # if result_undetermined.size > 0:
    #     hour_range = time_array_to_range(result_undetermined.values, "hour")
    #     for start, end in hour_range:
    #         start = start.strftime("%Y-%m-%d %H:%M:%S")
    #         end = end.strftime("%Y-%m-%d %H:%M:%S")
    #         print("Check hour: ", start, end)
    #         rest = self._execute_baseline(start_datetime=start, end_datetime=end)
    #         result["2m_temperature"].loc[f"{start}":f"{end}"] = rest[self.variable]
    #     result["2m_temperature"] = result["2m_temperature"].astype(bool)
    #     return result
    
    # def _get_range_min_max(self, _range, temporal_res):
    #     ds_min = []
    #     ds_max = []
    #     for start, end in _range:
    #         get_min_executor = cloud_get_raster_executor(
    #             variable=self.variable,
    #             start_datetime=start,
    #             end_datetime=end,
    #             temporal_resolution=temporal_res,
    #             min_lat=self.min_lat,
    #             max_lat=self.max_lat,
    #             min_lon=self.min_lon,
    #             max_lon=self.max_lon,
    #             spatial_resolution=self.spatial_resolution,
    #             aggregation="min",
    #         )
    #         get_max_executor = cloud_get_raster_executor(
    #             variable=self.variable,
    #             start_datetime=start,
    #             end_datetime=end,
    #             min_lat=self.min_lat,
    #             max_lat=self.max_lat,
    #             min_lon=self.min_lon,
    #             max_lon=self.max_lon,
    #             temporal_resolution=temporal_res,
    #             spatial_resolution=self.spatial_resolution,
    #             aggregation="max",
    #         )
    #         range_min = get_min_executor.execute()
    #         range_max = get_max_executor.execute()
    #         ds_min.append(range_min)
    #         ds_max.append(range_max)
    #     ds_min_concat = xr.concat(ds_min, dim="valid_time")
    #     ds_max_concat = xr.concat(ds_max, dim="valid_time")
    #     return ds_min_concat.compute(), ds_max_concat.compute()
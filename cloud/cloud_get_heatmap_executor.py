import numpy as np
import xarray as xr

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

    def execute(self):
        if self.aggregation == "mean":
            return self._get_mean_heatmap()
        elif self.aggreagtion == "max":
            return self._get_max_heatmap()
        elif self.aggregation == "min":
            return self._get_min_heatmap()
        else:
            return ValueError(f"Invalid aggregation method {self.aggregation}")
        

    def _get_mean_heatmap(self):
        year_range, month_range, day_range, hour_range = get_whole_ranges_between(
            self.start_datetime, self.end_datetime
        )
        ds_year = []
        ds_month = []
        ds_day = []
        ds_hour = []
        year_hours = []
        month_hours = []
        day_hours = []
        hour_hours = []
        
        for start_year, end_year in year_range:
            get_raster_year = cloud_get_raster_executor(
                self.variable,
                str(start_year),
                str(end_year),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="year",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_year.append(get_raster_year.execute())
            year_hours += [get_total_hours_in_year(y) for y in range(start_year.year, end_year.year + 1)]
        for start_month, end_month in month_range:
            get_raster_month = cloud_get_raster_executor(
                self.variable,
                str(start_month),
                str(end_month),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="month",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_month.append(get_raster_month.execute())
            year_hours += [get_total_hours_in_month(m) for m in range(start_month, end_month)]
        for start_day, end_day in day_range:
            get_raster_day = cloud_get_raster_executor(
                self.variable,
                str(start_day),
                str(end_day),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="day",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_day.append(get_raster_day.execute())
            year_hours += [24 for _ in range(number_of_days_inclusive(start_day, end_day))]
        for start_hour, end_hour in hour_range:
            get_raster_day = cloud_get_raster_executor(
                self.variable,
                str(start_hour),
                str(end_hour),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="hour",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_hour.append(get_raster_day.execute())
            year_hours += [1 for _ in range(number_of_days_inclusive(start_hour, end_hour))]
        
        xrds_concat = xr.concat(ds_year + ds_month + ds_day, + ds_hour, dim="valid_time")
        nd_array = xrds_concat[self.variable_short_name].to_numpy()
        weights = np.array(year_hours + month_hours + day_hours + hour_hours)
        total_hours = get_total_hours_between(self.start_datetime, self.end_datetime)
        weights = weights / total_hours
        average = np.average(nd_array, axis=0, weights=weights)
        res = xr.Dataset(
            {self.variable_short_name: (["latitude", "longitude"], average)},
            coords={"latitude": xrds_concat.latitude, "longitude": xrds_concat.longitude},
        )
        return res
    
    def _get_max_heatmap(self):
        year_range, month_range, day_range, hour_range = get_whole_ranges_between(
            self.start_datetime, self.end_datetime
        )
        ds_year = []
        ds_month = []
        ds_day = []
        ds_hour = []
        
        for start_year, end_year in year_range:
            get_raster_year = cloud_get_raster_executor(
                self.variable,
                str(start_year),
                str(end_year),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="year",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_year.append(get_raster_year.execute())
        for start_month, end_month in month_range:
            get_raster_month = cloud_get_raster_executor(
                self.variable,
                str(start_month),
                str(end_month),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="month",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_month.append(get_raster_month.execute())
        for start_day, end_day in day_range:
            get_raster_day = cloud_get_raster_executor(
                self.variable,
                str(start_day),
                str(end_day),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="day",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_day.append(get_raster_day.execute())
        for start_hour, end_hour in hour_range:
            get_raster_day = cloud_get_raster_executor(
                self.variable,
                str(start_hour),
                str(end_hour),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="hour",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_hour.append(get_raster_day.execute())
        
        return xr.concat(ds_year + ds_month + ds_day + ds_hour, dim="valid_time").max(dim="valid_time")
    
    def _get_min_heatmap(self):
        year_range, month_range, day_range, hour_range = get_whole_ranges_between(
            self.start_datetime, self.end_datetime
        )
        ds_year = []
        ds_month = []
        ds_day = []
        ds_hour = []
        
        for start_year, end_year in year_range:
            get_raster_year = cloud_get_raster_executor(
                self.variable,
                str(start_year),
                str(end_year),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="year",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_year.append(get_raster_year.execute())
        for start_month, end_month in month_range:
            get_raster_month = cloud_get_raster_executor(
                self.variable,
                str(start_month),
                str(end_month),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="month",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_month.append(get_raster_month.execute())
        for start_day, end_day in day_range:
            get_raster_day = cloud_get_raster_executor(
                self.variable,
                str(start_day),
                str(end_day),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="day",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_day.append(get_raster_day.execute())
        for start_hour, end_hour in hour_range:
            get_raster_day = cloud_get_raster_executor(
                self.variable,
                str(start_hour),
                str(end_hour),
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="hour",
                spatial_resolution=self.spatial_resolution,
                aggregation=self.aggregation,
            )
            ds_hour.append(get_raster_day.execute())
        
        return xr.concat(ds_year + ds_month + ds_day + ds_hour, dim="valid_time").min(dim="valid_time")
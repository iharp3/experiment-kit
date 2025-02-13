import pandas as pd
import xarray as xr

from utils.const import long_short_name_dict
from vanilla_get_raster_executor import VanillaGetRasterExecutor
from vanilla_get_timeseries_executor import VanillaGetTimeseriesExecutor
from utils.get_whole_period import get_whole_period_between, get_last_date_of_month, time_array_to_range

class VanillaFindTimeExecutor:
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
        self.variable_short_name = long_short_name_dict[self.variable]

    def execute(self):
        if self.temporal_resolution == "hour" and self.filter_predicate != "!=":
            return self._execute_pyramid_hour()
        return self._execute_baseline()
    
    def execute_baseline(self):
        return self._execute_baseline()

    def _execute_baseline(self, start_datetime=None, end_datetime=None):
        if start_datetime is None:
            start_datetime = self.start_datetime
        if end_datetime is None:
            end_datetime = self.end_datetime
        timeseries_executor = VanillaGetTimeseriesExecutor(
            self.variable,
            start_datetime,
            end_datetime,
            self.temporal_resolution,
            self.min_lat,
            self.max_lat,
            self.min_lon,
            self.max_lon,
            spatial_resolution=self.spatial_resolution,
            aggregation=self.aggregation,
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
            raise ValueError("Invalid filter_predicate")
        res = res.fillna(False)
        res = res.astype(bool)
        return res

    def _execute_pyramid_hour(self):

        years, months, days, hours = get_whole_period_between(self.start_datetime, self.end_datetime)

        time_points = pd.date_range(start=self.start_datetime, end=self.end_datetime, freq="h")
        result = xr.Dataset(
            data_vars={self.variable_short_name: (["time"], [None] * len(time_points))},
            coords=dict(time=time_points),
        )

        if years:
            year_range = time_array_to_range(years, "year")
            year_min, year_max = self._get_range_min_max(year_range, "year")
            for year in years:
                year_determined = False
                year_datetime = f"{year}-12-31 00:00:00"
                curr_year_min = year_min.sel(time=year_datetime)[self.variable_short_name].values.min()
                curr_year_max = year_max.sel(time=year_datetime)[self.variable_short_name].values.max()
                if self.filter_predicate == ">":
                    if curr_year_min > self.filter_value:
                        print(f"{year}: min > filter, True")
                        year_determined = True
                        result[self.variable_short_name].loc[str(year) : str(year)] = True
                    elif curr_year_max <= self.filter_value:
                        print(f"{year}: max <= filter, False")
                        year_determined = True
                        result[self.variable_short_name].loc[str(year) : str(year)] = False
                elif self.filter_predicate == "<":
                    if curr_year_min >= self.filter_value:
                        print(f"{year}: min >= filter, False")
                        year_determined = True
                        result[self.variable_short_name].loc[str(year) : str(year)] = False
                    elif curr_year_max < self.filter_value:
                        print(f"{year}: max < filter, True")
                        year_determined = True
                        result[self.variable_short_name].loc[str(year) : str(year)] = True
                elif self.filter_predicate == "==":
                    if curr_year_min > self.filter_value or curr_year_max < self.filter_value:
                        print(f"{year}: min > filter or max < filter, False")
                        year_determined = True
                        result[self.variable_short_name].loc[str(year) : str(year)] = False
                if not year_determined:
                    # add months to months
                    months = months + [f"{year}-{month:02d}" for month in range(1, 13)]

        if months:
            # update month_range
            month_range = time_array_to_range(months, "month")
            month_min, month_max = self._get_range_min_max(month_range, "month")
            for month in months:
                month_determined = False
                month_datetime = f"{month}-{get_last_date_of_month(pd.Timestamp(month))} 00:00:00"
                curr_month_min = month_min.sel(time=month_datetime)[self.variable_short_name].values.min()
                curr_month_max = month_max.sel(time=month_datetime)[self.variable_short_name].values.max()
                if self.filter_predicate == ">":
                    if curr_month_min > self.filter_value:
                        print(f"{month}: min > filter, True")
                        month_determined = True
                        result[self.variable_short_name].loc[month:month] = True
                    elif curr_month_max <= self.filter_value:
                        print(f"{month}: max <= filter, False")
                        month_determined = True
                        result[self.variable_short_name].loc[month:month] = False
                elif self.filter_predicate == "<":
                    if curr_month_min >= self.filter_value:
                        print(f"{month}: min >= filter, False")
                        month_determined = True
                        result[self.variable_short_name].loc[month:month] = False
                    elif curr_month_max < self.filter_value:
                        print(f"{month}: max < filter, True")
                        month_determined = True
                        result[self.variable_short_name].loc[month:month] = True
                elif self.filter_predicate == "==":
                    if curr_month_min > self.filter_value or curr_month_max < self.filter_value:
                        print(f"{month}: min > filter or max < filter, False")
                        month_determined = True
                        result[self.variable_short_name].loc[month:month] = False
                if not month_determined:
                    # add days to days
                    days = days + [
                        f"{month}-{day:02d}" for day in range(1, get_last_date_of_month(pd.Timestamp(month)) + 1)
                    ]

        if days:
            day_range = time_array_to_range(days, "day")
            day_min, day_max = self._get_range_min_max(day_range, "day")
            for day in days:
                day_datetime = f"{day} 00:00:00"
                curr_day_min = day_min.sel(time=day_datetime)[self.variable_short_name].values.min()
                curr_day_max = day_max.sel(time=day_datetime)[self.variable_short_name].values.max()
                if self.filter_predicate == ">":
                    if curr_day_min > self.filter_value:
                        print(f"{day}: min > filter, True")
                        result[self.variable_short_name].loc[day:day] = True
                    elif curr_day_max <= self.filter_value:
                        print(f"{day}: max <= filter, False")
                        result[self.variable_short_name].loc[day:day] = False
                elif self.filter_predicate == "<":
                    if curr_day_min >= self.filter_value:
                        print(f"{day}: min >= filter, False")
                        result[self.variable_short_name].loc[day:day] = False
                    elif curr_day_max < self.filter_value:
                        print(f"{day}: max < filter, True")
                        result[self.variable_short_name].loc[day:day] = True
                elif self.filter_predicate == "==":
                    if curr_day_min > self.filter_value or curr_day_max < self.filter_value:
                        print(f"{day}: min > filter or max < filter, False")
                        result[self.variable_short_name].loc[day:day] = False

        result_undetermined = result["time"].where(result[self.variable_short_name].isnull(), drop=True)
        if result_undetermined.size > 0:
            hour_range = time_array_to_range(result_undetermined.values, "hour")
            for start, end in hour_range:
                start = start.strftime("%Y-%m-%d %H:%M:%S")
                end = end.strftime("%Y-%m-%d %H:%M:%S")
                print("Check hour: ", start, end)
                rest = self._execute_baseline(start_datetime=start, end_datetime=end)
                result[self.variable_short_name].loc[f"{start}":f"{end}"] = rest[self.variable_short_name]
        result[self.variable_short_name] = result[self.variable_short_name].astype(bool)
        return result

    def _get_range_min_max(self, _range, temporal_res):
        ds_min = []
        ds_max = []
        for start, end in _range:
            get_min_executor = VanillaGetRasterExecutor(
                variable=self.variable,
                start_datetime=start,
                end_datetime=end,
                min_lat=self.min_lat,
                max_lat=self.max_lat,
                min_lon=self.min_lon,
                max_lon=self.max_lon,
                temporal_resolution=temporal_res,
                aggregation="min",
            )
            get_max_executor = VanillaGetRasterExecutor(
                variable=self.variable,
                start_datetime=start,
                end_datetime=end,
                min_lat=self.min_lat,
                max_lat=self.max_lat,
                min_lon=self.min_lon,
                max_lon=self.max_lon,
                temporal_resolution=temporal_res,
                aggregation="max",
            )
            range_min = get_min_executor.execute()
            range_max = get_max_executor.execute()
            ds_min.append(range_min)
            ds_max.append(range_max)
        ds_min_concat = xr.concat(ds_min, dim="time")
        ds_max_concat = xr.concat(ds_max, dim="time")
        return ds_min_concat.compute(), ds_max_concat.compute()


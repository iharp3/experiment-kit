import xarray as xr


def get_file_list(start_datetime, end_datetime):
    file_list = []
    start_year = start_datetime[:4]
    end_year = end_datetime[:4]
    for year in range(int(start_year), int(end_year) + 1):
        file_path = f"/era5/raw/2m_temperature/2m_temperature-{year}.nc"
        file_list.append(file_path)
    print(file_list)
    return file_list


class vanilla_get_timeseries_executor:
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
        aggregation: str,  # "mean", "max", "min"  !! Use this aggregation for timeseries aggregation as well
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
        file_list = get_file_list(self.start_datetime, self.end_datetime)
        ds_list = []
        for file in file_list:
            ds = xr.open_dataset(file, engine="netcdf4").sel(
                time=slice(start_datetime, end_datetime),
                latitude=slice(max_lat, min_lat),
                longitude=slice(min_lon, max_lon),
            )
            ds_list.append(ds)
        ds = xr.concat([i.chunk() for i in ds_list], dim="time")
        return ds

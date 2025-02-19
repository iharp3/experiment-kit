import xarray as xr


def get_file_list(start_datetime, end_datetime):
    file_list = []
    start_year = start_datetime[:4]
    end_year = end_datetime[:4]
    for year in range(int(start_year), int(end_year) + 1):
        file_path = f"/data/2m_temperature-{year}.nc"
        file_list.append(file_path)
    return file_list


def temporal_resolution_to_freq(resolution):
    if resolution == "hour":
        return "h"
    elif resolution == "day":
        return "D"
    elif resolution == "month":
        return "ME"
    elif resolution == "year":
        return "YE"
    else:
        raise ValueError("Invalid temporal_resolution")


class VanillaGetRasterExecutor:
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
        aggregation: str,  # "mean", "max", "min"
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
        """
        Will be run on cs-u-spatial-514.cs.umn.edu
        Only use the raw data in :/era5/raw/2m_temperature, See README.md
        """
        file_list = get_file_list(self.start_datetime, self.end_datetime)
        ds_list = []
        for file in file_list:
            ds = xr.open_dataset(file, engine="netcdf4").sel(
                time=slice(self.start_datetime, self.end_datetime),
                latitude=slice(self.max_lat, self.min_lat),
                longitude=slice(self.min_lon, self.max_lon),
            )
            ds_list.append(ds)
        ds = xr.concat([i.chunk() for i in ds_list], dim="time")

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
        return ds.load()

    def execute_dask(self):
        """
        Will not use this function in the vanilla baseline
        """
        file_list = get_file_list(self.start_datetime, self.end_datetime)
        ds = xr.open_mfdataset(
            file_list,
            engine="netcdf4",
            chunks={"time": 1000, "latitude": 100, "longitude": 100},
        ).sel(
            time=slice(self.start_datetime, self.end_datetime),
            latitude=slice(self.max_lat, self.min_lat),
            longitude=slice(self.min_lon, self.max_lon),
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
        return ds.load()

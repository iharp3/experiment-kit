import xarray as xr

ds = xr.open_dataset("download.nc")
era5_encoding = ds.t2m.encoding
data_folder = "/data/era5/agg/2m_temperature"

if __name__ == "__main__":
    ds = xr.open_mfdataset("/data/era5/agg/2m_temperature/2m_temperature-20*_100.nc", combine="by_coords")
    ds.t2m.encoding = era5_encoding
    ds.to_netcdf(f"/data/era5/agg/2m_temperature/2m_temperature-hour_100.nc")

    ds = xr.open_mfdataset("/data/era5/agg/2m_temperature/2m_temperature-20*_50.nc", combine="by_coords")
    ds.t2m.encoding = era5_encoding
    ds.to_netcdf(f"/data/era5/agg/2m_temperature/2m_temperature-hour_50.nc")

from tile.utils import get_agg_function
from tile.tiledb_get_raster_executor import tiledb_get_raster_executor

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
        heatmap_aggregation_method: str,
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
        executor = tiledb_get_raster_executor(
            variable= self.variable,
            start_datetime= self.start_datetime,
            end_datetime= self.end_datetime,
            temporal_resolution= self.temporal_resolution,
            min_lat= self.min_lat,
            max_lat= self.max_lat,
            min_lon= self.min_lon,
            max_lon= self.max_lon,
            spatial_resolution= self.spatial_resolution,
            aggregation= self.aggregation,
        )

        raster = executor.execute()
        agg_function = get_agg_function(self.aggregation)

        result = agg_function(raster, axis=0)   # Shape: (lat, lon)
        
        # print(f"\n\t heatmap result: {result.shape}")
        return result
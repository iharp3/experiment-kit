import json
import tiledb
from tiledb_get_raster_executor import tiledb_get_raster_executor

json_file = "/data/experiment-kit/tiledb/config.json"
with open(json_file, "r") as f:
    inputs = json.load(f),

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
        # get_raster = tiledb_get_raster_executor(
        #     variable= self.variable,
        #     start_datetime= self.start_datetime,
        #     end_datetime= self.end_datetime,
        #     temporal_resolution= "hour",
        #     min_lat= self.min_lat,
        #     max_lat= self.max_lat,
        #     min_lon= self.min_lon,
        #     max_lon= self.max_lon,
        #     spatial_resolution= self.spatial_resolution,
        #     aggregation= self.aggregation,
        # )
        # ds = get_raster.execute()
        # ----
        # with tiledb.open(inputs["tiledb_data_dir"], mode="r") as array:
            # if self.temporal_resolution == "hour":
            #     return self._get_hour
            # daily_means = []
        
        # for day in range(365):  # Loop over days
        #     start_time = day * 24
        #     end_time = start_time + 24  # 24-hour slice

        #     # Aggregate over the 24-hour slice
        #     daily_mean = array[start_time:end_time].agg("mean")["temperature"]
        #     daily_means.append(daily_mean)

        # Convert to NumPy array
        # daily_means = np.array(daily_means)
        #-----

        if self.aggregation == "mean":
            return self._get_mean_heatmap()
        elif self.aggregation == "max":
            return self._get_max_heatmap()
        elif self.aggregation == "min":
            return self._get_min_heatmap()
        else:
            return ValueError(f"Invalid aggregation method {self.aggregation}")
        
    def _get_mean_heatmap(self, d):
        pass

    def _get_max_heatmap(self, d):
        pass

    def _get_min_heatmap(self, d):
        pass
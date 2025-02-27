import pandas as pd
import sys
import time
import os

main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))    # /../experiment-kit
sys.path.append(main_dir)

from proposed.query_executor_heatmap import HeatmapExecutor
from proposed.query_executor_find_time2 import FindTimeExecutor

from vanilla.vanilla_get_heatmap_executor import VanillaGetHeatmapExecutor
from vanilla.vanilla_find_time_executor import VanillaFindTimeExecutor

# from tile.tiledb_get_heatmap_executor import tiledb_get_heatmap_executor
# from tile.tiledb_find_time_executor import tiledb_find_time_executor

systems_list = ["proposed", "vanilla"]
print("all modules loaded")
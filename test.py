# %%
import pathlib
import pandas as pd
import dask.dataframe as dd
from config import load_config
import visualizations
import numpy as np
import utils
config = load_config()

origin_files = pathlib.Path(r"E:\Torrent\10G_data_new\part-00000.parquet")
origin_files = pathlib.Path(r"E:\Torrent\30G_data_new")
processed_files = pathlib.Path("processed_data") / origin_files.name
processed_time_files = pathlib.Path("processed_data_1") / origin_files.name

# %%
df = dd.read_parquet(origin_files)
visualizations.draw_violinplot(df,"income")
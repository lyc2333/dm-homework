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
# origin_files = pathlib.Path(r"E:\Torrent\30G_data_new")

origin_files = pathlib.Path(r"E:\data_mining\dm-homework\processed_data_2\30G_data_new")
origin_files = pathlib.Path(r"E:\data_mining\dm-homework\processed_data_2\10G_data_new\part-00000.parquet")
origin_files = pathlib.Path(r"E:\Torrent\10G_data_new\part-00000.parquet")

origin_files = pathlib.Path(r"E:\Torrent\30G_data_new")

processed_files = pathlib.Path("processed_data") / origin_files.name
processed_time_files = pathlib.Path("processed_data_1") / origin_files.name

# %%
df0 = dd.read_parquet(origin_files)
df0 = df0.repartition(npartitions=1).reset_index(drop=True)
purchased_items_files = pathlib.Path("processed_data_1") / origin_files.name

df1 = dd.read_parquet(purchased_items_files).drop(['id'], axis=1).repartition(npartitions=1).reset_index(drop=True)
df = dd.concat([df0,df1],axis=1)
# visualizations.draw_boxplot(df, 'age')
# visualizations.draw_violinplot(df, 'age')
visualizations.draw_stacked_barplot(df,"country","categories")
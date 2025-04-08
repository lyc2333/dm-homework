import json
import pyarrow
from typing import TypedDict

class Config(TypedDict):
    datapath: str
    

def load_config(config_path="./config.json")->Config:
    with open(config_path) as config_file:
        config = json.load(config_file)
    return config

import pyarrow.parquet as pq
import pathlib

if __name__=="__main__":
    config=load_config()
    datapath=pathlib.Path(config["datapath"])
    parquet_file = pq.ParquetFile(datapath/"part-00001.parquet")
    for i in parquet_file.iter_batches(batch_size=5):
        print("RecordBatch")
        df=i.to_pandas()
        for index, row in df.iterrows():
            print(row)
            print(json.loads(row["purchase_history"]))
        
        break


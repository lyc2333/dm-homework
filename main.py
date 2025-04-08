import pandas as pd
import pyarrow.parquet as pq
import pathlib
import json
import pyarrow
from typing import TypedDict
import dask.dataframe as dd


class Config(TypedDict):
    datapath: str


def load_config(config_path="./config.json") -> Config:
    with open(config_path) as config_file:
        config = json.load(config_file)
    return config


def extract_fields(df):
    def parse_json(s):
        try:
            d = json.loads(s)
            return pd.Series({
                'average_price': d.get('average_price', None),
                'category': d.get('category', None),
                'item_count': len(d.get('items', [])),
            })
        except Exception:
            return pd.Series({'average_price': None, 'category': None, 'item_count': None})

    extracted = df['purchase_history'].apply(parse_json)
    return df.join(extracted)


def parse_json(row):
    d = json.loads(row["purchase_history"])

    return pd.Series({
        'average_price': d.get('average_price', None),
        'category': d.get('category', None),
        'item_count': len(d.get('items', [])),
    })


if __name__ == "__main__":
    config = load_config()
    datapath = pathlib.Path(config["datapath"])
    parquet_file = pq.ParquetFile(datapath/"part-00001.parquet")
    ddf = dd.read_parquet(datapath)
    
    ddf_extracted = ddf.apply(parse_json,axis=1, meta={'average_price': 'f8', 'category': 'object', 'item_count': 'i8'})

    print(ddf_extracted.head(1))
    ddf_extracted.to_parquet('processed_data/1G_data', engine='pyarrow')
    #a=ddf_extracted.average_price.mean().compute()
    #print(a)
    # ddf.assign(purchase_history=lambda x:x["purchase_history"]).apply(json.loads)
    print(ddf.dtypes)
    # print(ddf.age.describe().compute())
    check_field = "age"
    q1 = ddf[check_field].quantile(0.25).compute()
    q3 = ddf[check_field].quantile(0.75).compute()
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    # 计算异常值数量
    outliers = ((ddf[check_field] < lower_bound) | (ddf[check_field] > upper_bound)).sum().compute()

    print(f"收入异常值数量: {outliers}, 占比: {outliers / len(ddf)*100:.2f}%")

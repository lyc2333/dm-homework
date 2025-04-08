import dask.dataframe as dd
import pandas as pd
import json

# 假设你已有一个 Pandas DataFrame
pdf = pd.DataFrame({
    'json_str': [
        '{"average_price":15.94,"category":"家居","items":[{"id":631},{"id":762}]}',
        '{"average_price":8.25,"category":"电子","items":[{"id":111},{"id":222},{"id":333}]}',
    ]
})

# 转成 Dask DataFrame
ddf = dd.from_pandas(pdf, npartitions=1)

# 处理函数：对每个 partition 处理，提取字段
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

    extracted = df['json_str'].apply(parse_json)
    return df.join(extracted)

# 应用到 Dask DataFrame
ddf_extracted = ddf.map_partitions(extract_fields)

# 查看结果
print(ddf_extracted.compute())

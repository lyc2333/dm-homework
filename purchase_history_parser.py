import json
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from tqdm import tqdm


def parse_purchase_history(input_path:str,output_path:str,batch_size:int=1024):
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


    # 打开 Parquet 文件
    parquet_file = pq.ParquetFile(input_path)

    # 初始化写入器
    writer = None


    total_rows = sum(parquet_file.metadata.row_group(i).num_rows for i in range(parquet_file.num_row_groups))

    # 计算总批次数
    total_batches = total_rows // batch_size + (1 if total_rows % batch_size > 0 else 0)


    for batch in tqdm(parquet_file.iter_batches(batch_size=batch_size), total=total_batches, desc="Processing Batches"):  # 每批读取 1024 行
        table = pa.Table.from_batches([batch])

        # 转为 pandas 处理（可选）
        df = table.to_pandas()

        # 对某些字段进行处理
        new_df = df["purchase_history"].apply(parse_json)

        # 转回 PyArrow Table
        new_table = pa.Table.from_pandas(new_df)

        # 写入新文件
        if writer is None:
            writer = pq.ParquetWriter(output_path, new_table.schema)
        writer.write_table(new_table)

    # 关闭 writer
    if writer:
        writer.close()

parse_purchase_history("1G_data\\part-00001.parquet","processed_data\\1G_data\\part-00001.parquet",2048)
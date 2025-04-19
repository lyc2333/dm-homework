import multiprocessing
from typing import Any, Callable
from config import load_config
import pathlib
import orjson as json
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from tqdm import tqdm
from datetime import datetime


global_set = set()
def check_column(input_path: str | pathlib.Path, column: str, batch_size: int = 1024, process_id: int = 0):
    input_path = pathlib.Path(input_path)
    # 打开 Parquet 文件
    parquet_file = pq.ParquetFile(input_path)

    # 初始化写入器
    writer = None

    total_rows = sum(parquet_file.metadata.row_group(i).num_rows for i in range(parquet_file.num_row_groups))

    # 计算总批次数
    total_batches = total_rows // batch_size + (1 if total_rows % batch_size > 0 else 0)

    for batch in tqdm(parquet_file.iter_batches(batch_size=batch_size, columns=["id", column]), total=total_batches, desc=input_path.name, position=process_id):

        # 多线程处理进度条有时会乱跳，但勉强能看，就不管了

        table = pa.Table.from_batches([batch])

        # 转为 pandas 处理
        df = table.to_pandas()

        # 对某些字段进行处理
        new_df = df[column]

        for row in new_df:
            row=json.loads(row)
            global_set.update(row['devices'])

        

if __name__ == "__main__":
    config = load_config()
    input_dir = pathlib.Path(config['datapath_10G'])
    num_file = 8
    for i in range(num_file):
        check_column(
            input_dir/f"part-{i:05d}.parquet",
            "login_history",
        )
    print(global_set)
    # 10G : {'desktop', 'mobile', 'tablet'} {'home', 'travel', 'work'}
    # 30G : {'desktop', 'mobile', 'tablet'} {'home', 'travel', 'work'}
    # multiprocessing.set_start_method('spawn')  # GPT说可以增加跨平台稳定性，没有测过
    # config = load_config()

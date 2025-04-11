import multiprocessing
from config import load_config
import pathlib
import json
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from tqdm import tqdm
from datetime import datetime


def parse_purchase_history(input_path: str | pathlib.Path, output_path: str | pathlib.Path, batch_size: int = 1024, save_item_list: bool = False, process_id: int = 0):
    input_path = pathlib.Path(input_path)
    output_path = pathlib.Path(output_path)

    def parse_json(s):
        try:
            d = json.loads(s)
            items = d.get('items', [])
            if save_item_list:
                res = pd.Series({
                    'average_price': d.get('average_price', None),
                    'category': d.get('category', None),
                    'items': json.dumps(items),
                    'item_count': len(items),
                })
            else:
                res = pd.Series({
                    'average_price': d.get('average_price', None),
                    'category': d.get('category', None),
                    'item_count': len(items),
                })
            return res
        except Exception:
            return pd.Series({'average_price': None, 'category': None, 'item_count': None})

    # 打开 Parquet 文件
    parquet_file = pq.ParquetFile(input_path)

    # 初始化写入器
    writer = None

    total_rows = sum(parquet_file.metadata.row_group(i).num_rows for i in range(parquet_file.num_row_groups))

    # 计算总批次数
    total_batches = total_rows // batch_size + (1 if total_rows % batch_size > 0 else 0)

    for batch in tqdm(parquet_file.iter_batches(batch_size=batch_size, columns=["purchase_history"]), total=total_batches, desc=input_path.name, position=process_id):

        # 多线程处理进度条有时会乱跳，但勉强能看，就不管了

        table = pa.Table.from_batches([batch])

        # 转为 pandas 处理
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


def parse_time(input_path: str | pathlib.Path, output_path: str | pathlib.Path, batch_size: int = 1024, process_id: int = 0):
    input_path = pathlib.Path(input_path)
    output_path = pathlib.Path(output_path)

    def apply_func(x):
        return pd.Series({
            'timestamp_num': datetime.fromisoformat(x["timestamp"]).timestamp(),
            'registration_date_num': datetime.strptime(x["registration_date"], '%Y-%m-%d').timestamp()
        })

    # 打开 Parquet 文件
    parquet_file = pq.ParquetFile(input_path)

    # 初始化写入器
    writer = None

    total_rows = sum(parquet_file.metadata.row_group(i).num_rows for i in range(parquet_file.num_row_groups))

    # 计算总批次数
    total_batches = total_rows // batch_size + (1 if total_rows % batch_size > 0 else 0)

    for batch in tqdm(parquet_file.iter_batches(batch_size=batch_size, columns=["timestamp", "registration_date"]), total=total_batches, desc=input_path.name, position=process_id):

        # 多线程处理进度条有时会乱跳，但勉强能看，就不管了

        table = pa.Table.from_batches([batch])

        # 转为 pandas 处理
        df = table.to_pandas()

        # 对某些字段进行处理
        new_df = df.apply(apply_func, axis=1)

        # 转回 PyArrow Table
        new_table = pa.Table.from_pandas(new_df)

        # 写入新文件
        if writer is None:
            writer = pq.ParquetWriter(output_path, new_table.schema)
        writer.write_table(new_table)

    # 关闭 writer
    if writer:
        writer.close()


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')  # GPT说可以增加跨平台稳定性，没有测过
    config = load_config()

    num_file = 16
    input_dir = pathlib.Path(config['datapath'])
    output_dir = pathlib.Path("processed_data") / input_dir.name
    output_dir.mkdir(parents=True, exist_ok=True)
    processes: list[multiprocessing.Process] = []

    for i in range(num_file):
        filename = f"part-{i:05d}.parquet"
        t = multiprocessing.Process(target=parse_purchase_history, args=(input_dir / filename, output_dir / filename, 1024, False, i))
        processes.append(t)
        t.start()
    for t in processes:
        t.join()
    tqdm.write("所有任务完成")
    

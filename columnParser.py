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


def parse_purchase_history(s):
    try:
        d = json.loads(s)
        items = d.get('items', [])

        res = pd.Series({
            'avg_price': float(d.get('avg_price', None)),
            'categories': d.get('categories', None),
            'items': json.dumps(items),
            'item_count': len(items),
            'payment_method': d.get('payment_method', None),
            'payment_status': d.get('payment_status', None),
            'purchase_date': datetime.strptime(d.get('purchase_date', None), '%Y-%m-%d').timestamp()
        })

        return res
    except Exception:
        tqdm.write(f"error when parse {s}")
        return pd.Series({
            'avg_price': None,
            'category': None,
            'items': None,
            'item_count': None,
            'payment_method': None,
            'payment_status': None,
            'purchase_date': None
        })


def parse_login_history(s):
    try:
        d = json.loads(s)
        items = d.get('items', [])
        devices = set(d.get('devices', []))
        locations = set(d.get('locations', []))

        res = pd.Series({
            'avg_session_duration': float(d.get('avg_session_duration', None)),
            'devices_desktop': 'desktop' in devices,
            'devices_mobile': 'mobile' in devices,
            'devices_tablet': 'tablet' in devices,
            'first_login': datetime.strptime(d.get('first_login', None), '%Y-%m-%d').timestamp(),
            'locations_home': 'home' in locations,
            'locations_travel': 'travel' in locations,
            'locations_work': 'work' in locations,
            'login_count': d.get('login_count', None),
            'timestamps': json.dumps(d.get('timestamps', [])),
        })

        return res
    except Exception:
        tqdm.write(f"error when parse {s}")
        return pd.Series({
            'avg_session_duration': None,
            'devices_desktop': None,
            'devices_mobile': None,
            'devices_tablet': None,
            'first_login': None,
            'locations_home': None,
            'locations_travel': None,
            'locations_work': None,
            'login_count': None,
            'timestamps': None,
        })


ADDRESS_DICT = {
    "黑龙": "黑龙江",
    "内蒙": "内蒙古",
    "No": "Non-Chinese"
}


def parse_address(s):
    try:
        temp = s[0:2]
        return pd.Series({
            'province': ADDRESS_DICT.get(temp, temp),

        })
    except Exception:
        tqdm.write(f"error when parse {s}")
        return pd.Series({
            'province': None,
        })


def parse_json_column(input_path: str | pathlib.Path, output_path: str | pathlib.Path, schema: Any, column: str, parse_func: Callable, batch_size: int = 1024, process_id: int = 0):
    input_path = pathlib.Path(input_path)
    output_path = pathlib.Path(output_path)
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
        new_df = df[column].apply(parse_func)

        # 添加 id 字段
        new_df["id"] = df["id"]

        # 转回 PyArrow Table
        new_table = pa.Table.from_pandas(new_df)

        # 写入新文件
        if writer is None:
            if schema is None:
                schema = new_table.schema
            if "id" not in schema.names:
                schema = pa.schema(list(schema) + [pa.field("id", pa.int64())])  # 添加id字段

            writer = pq.ParquetWriter(output_path, schema)
        writer.write_table(new_table)

    # 关闭 writer
    if writer:
        writer.close()



def parse_purchase_history_main(input_path: str, output_path: str = "processed_data_1", num_file: int = 8):

    input_dir = pathlib.Path(input_path)
    output_dir = pathlib.Path(output_path) / input_dir.name
    output_dir.mkdir(parents=True, exist_ok=True)
    args = [
        (
            input_dir / f"part-{i:05d}.parquet",
            output_dir / f"part-{i:05d}.parquet",
            pa.schema([
                pa.field('avg_price', pa.float64()),
                pa.field('categories', pa.string()),
                pa.field('items', pa.binary()),
                pa.field('item_count', pa.int64()),
                pa.field('payment_method', pa.string()),
                pa.field('payment_status', pa.string()),
                pa.field('purchase_date', pa.float64()),
            ]),
            "purchase_history",
            parse_purchase_history,
            1024,
            i,
        ) for i in range(num_file)
    ]

    with multiprocessing.Pool() as pool:
        pool.starmap(parse_json_column, args)


def parse_login_history_main(input_path: str, output_path: str = "processed_data_1", num_file: int = 8):

    input_dir = pathlib.Path(input_path)
    output_dir = pathlib.Path(output_path) / input_dir.name
    output_dir.mkdir(parents=True, exist_ok=True)

    args = [
        (
            input_dir / f"part-{i:05d}.parquet",
            output_dir / f"part-{i:05d}.parquet",
            pa.schema([
                pa.field('avg_session_duration', pa.float64()),
                pa.field('devices_desktop', pa.bool_()),
                pa.field('devices_mobile', pa.bool_()),
                pa.field('devices_tablet', pa.bool_()),
                pa.field('first_login', pa.float64()),
                pa.field('locations_home', pa.bool_()),
                pa.field('locations_travel', pa.bool_()),
                pa.field('locations_work', pa.bool_()),
                pa.field('login_count', pa.int64()),
                pa.field('timestamps', pa.binary()),
            ]),
            "login_history",
            parse_login_history,
            1024,
            i,
        ) for i in range(num_file)
    ]

    with multiprocessing.Pool() as pool:
        pool.starmap(parse_json_column, args)

def parse_address_main(input_path: str, output_path: str = "processed_data_3", num_file: int = 8):

    input_dir = pathlib.Path(input_path)
    output_dir = pathlib.Path(output_path) / input_dir.name
    output_dir.mkdir(parents=True, exist_ok=True)

    args = [
        (
            input_dir / f"part-{i:05d}.parquet",  # input_dir / f"part-{i:05d}.parquet",
            output_dir / f"part-{i:05d}.parquet",
            pa.schema([
                pa.field('province', pa.string()),

            ]),
            "address",
            parse_address,
            1024,
            i,
        ) for i in range(num_file)
    ]

    with multiprocessing.Pool() as pool:
        pool.starmap(parse_json_column, args)


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')  # GPT说可以增加跨平台稳定性，没有测过
    config = load_config()
    parse_purchase_history_main(config['datapath_10G'], "processed_data_1", 8)
    parse_login_history_main(config['datapath_10G'], "processed_data_2", 8)
    parse_address_main(config['datapath_10G'], "processed_data_3", 8)
    
    parse_purchase_history_main(config['datapath_30G'], "processed_data_1", 16)
    parse_login_history_main(config['datapath_30G'], "processed_data_2", 16)
    parse_address_main(config['datapath_30G'], "processed_data_3", 16)

    tqdm.write("所有任务完成")
